from __future__ import annotations
import requests, time, os, re, sys, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from TSRUrl import TSRUrl
from logger import logger
from exceptions import *


CHUNK_SIZE = 1024 * 512        # 512 KB por chunk de escritura
NUM_CONNECTIONS = 8            # Conexiones paralelas para descarga
MIN_SPLIT_SIZE = 1024 * 1024   # Mínimo 1 MB para usar descarga paralela


def stripForbiddenCharacters(string: str) -> str:
    return re.sub('[\\<>/:"|?*]', "", string)


class DownloadProgress:
    """Verbose de progreso en una sola línea con barra, %, velocidad."""

    def __init__(self, total_bytes: int, starting_bytes: int, file_name: str):
        self.total = total_bytes + starting_bytes
        self.downloaded = starting_bytes
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.file_name = file_name
        self._last_print = 0.0

    def add(self, amount: int):
        with self.lock:
            self.downloaded += amount
            now = time.time()
            if now - self._last_print >= 0.2 or (self.total > 0 and self.downloaded >= self.total):
                self._print()
                self._last_print = now

    def _print(self):
        elapsed = max(time.time() - self.start_time, 0.001)
        speed = self.downloaded / elapsed

        pct = (self.downloaded / self.total * 100) if self.total > 0 else 0.0
        bar_len = 30
        filled = int(bar_len * pct / 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        line = (
            f"\r  [{bar}] {pct:5.1f}%  "
            f"{self._fmt_bytes(self.downloaded)}/{self._fmt_bytes(self.total)}  "
            f"{self._fmt_speed(speed)}  "
            f"{self.file_name[:40]}"
        )
        sys.stdout.write(line)
        sys.stdout.flush()

        if self.total > 0 and self.downloaded >= self.total:
            sys.stdout.write("\n")
            sys.stdout.flush()

    @staticmethod
    def _fmt_bytes(b: int) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"

    @staticmethod
    def _fmt_speed(bps: float) -> str:
        for unit in ("B/s", "KB/s", "MB/s", "GB/s"):
            if bps < 1024:
                return f"{bps:.1f} {unit}"
            bps /= 1024
        return f"{bps:.1f} TB/s"


class TSRDownload:
    @classmethod
    def __init__(self, url: TSRUrl, sessionId: str):
        self.session: requests.Session = requests.Session()
        self.session.cookies.set("tsrdlsession", sessionId)
        self.url: TSRUrl = url
        self.ticket: str = ""
        self.__initDownload()

    @classmethod
    def download(self, downloadPath: str) -> str:
        logger.info(f"Starting download for: {self.url.url}")

        # El timer de 4s es SOLO del lado del cliente (JS). El servidor no valida tiempo.
        # Llamamos getdownloadurl directamente con el ticket obtenido en initDownload.
        downloadUrl = self.__getDownloadUrl()
        logger.debug(f"Got downloadUrl: {downloadUrl}")

        # Un GET con stream=True nos da Content-Length, Accept-Ranges y Content-Disposition
        initResp = self.session.get(downloadUrl, stream=True, allow_redirects=True)
        total_size = int(initResp.headers.get("Content-Length", 0))
        accepts_ranges = initResp.headers.get("Accept-Ranges", "none").lower() == "bytes"
        content_disp = initResp.headers.get("Content-Disposition", "")
        initResp.close()

        logger.debug(f"Total size: {total_size} bytes | Accept-Ranges: {accepts_ranges}")

        fileName = stripForbiddenCharacters(self.__parseFileName(content_disp, downloadUrl))
        logger.debug(f"Got fileName: {fileName}")

        partPath = f"{downloadPath}/{fileName}.part"
        finalPath = f"{downloadPath}/{fileName}"

        startingBytes = os.path.getsize(partPath) if os.path.exists(partPath) else 0
        logger.debug(f"Got startingBytes: {startingBytes}")

        if accepts_ranges and total_size > MIN_SPLIT_SIZE:
            self.__downloadParallel(downloadUrl, partPath, startingBytes, total_size, fileName)
        else:
            self.__downloadSequential(downloadUrl, partPath, startingBytes, total_size, fileName)

        logger.debug(f"Finalizando archivo: {fileName}")
        if os.path.exists(finalPath):
            os.replace(partPath, finalPath)
        else:
            os.rename(partPath, finalPath)

        return fileName

    # ------------------------------------------------------------------ #
    #  Descarga secuencial (fallback si el servidor no soporta ranges)    #
    # ------------------------------------------------------------------ #
    @classmethod
    def __downloadSequential(self, downloadUrl, partPath, startingBytes, total_size, fileName):
        logger.info("Usando descarga secuencial")
        progress = DownloadProgress(total_size, startingBytes, fileName)

        resp = self.session.get(
            downloadUrl,
            stream=True,
            headers={"Range": f"bytes={startingBytes}-"},
        )
        logger.debug(f"Request status: {resp.status_code}")

        with open(partPath, "ab") as f:
            for chunk in resp.iter_content(CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    progress.add(len(chunk))

    # ------------------------------------------------------------------ #
    #  Descarga paralela con múltiples Range requests simultáneos         #
    # ------------------------------------------------------------------ #
    @classmethod
    def __downloadParallel(self, downloadUrl, partPath, startingBytes, total_size, fileName):
        logger.info(f"Usando descarga paralela con {NUM_CONNECTIONS} conexiones")

        remaining = total_size - startingBytes
        chunk_per_thread = remaining // NUM_CONNECTIONS
        progress = DownloadProgress(total_size, startingBytes, fileName)
        buffers: dict[int, bytes] = {}
        lock = threading.Lock()

        ranges: list[tuple[int, int]] = []
        start = startingBytes
        for i in range(NUM_CONNECTIONS):
            end = start + chunk_per_thread - 1 if i < NUM_CONNECTIONS - 1 else total_size - 1
            ranges.append((start, end))
            start = end + 1

        def fetch_range(idx: int, byte_start: int, byte_end: int) -> tuple[int, bytes]:
            sess = requests.Session()
            sess.cookies.update(self.session.cookies)
            data = bytearray()
            resp = sess.get(
                downloadUrl,
                headers={"Range": f"bytes={byte_start}-{byte_end}"},
                stream=True,
                timeout=60,
            )
            for chunk in resp.iter_content(CHUNK_SIZE):
                if chunk:
                    data.extend(chunk)
                    progress.add(len(chunk))
            return idx, bytes(data)

        with ThreadPoolExecutor(max_workers=NUM_CONNECTIONS) as executor:
            futures = {executor.submit(fetch_range, i, r[0], r[1]): i for i, r in enumerate(ranges)}
            for future in as_completed(futures):
                idx, data = future.result()
                with lock:
                    buffers[idx] = data

        with open(partPath, "ab") as f:
            for i in range(NUM_CONNECTIONS):
                f.write(buffers[i])

        logger.debug("Todos los segmentos escritos correctamente")

    # ------------------------------------------------------------------ #
    #  Helpers                                                            #
    # ------------------------------------------------------------------ #
    @classmethod
    def __parseFileName(self, content_disp: str, downloadUrl: str) -> str:
        match = re.search(r'(?<=filename=").+(?=")', content_disp)
        if match:
            return match[0]
        return downloadUrl.split("/")[-1].split("?")[0]

    @classmethod
    def __getDownloadUrl(self) -> str:
        """
        Replica _dl() de local.js: GET getdownloadurl con itemid + ticket.
        El timer de 4s es puramente client-side; el servidor NO lo valida.
        """
        url = (
            f"https://www.thesimsresource.com/ajax.php"
            f"?c=downloads&a=getdownloadurl&ajax=1"
            f"&itemid={self.url.itemId}&mid=0&lk=0"
        )
        if self.ticket:
            url += f"&ticket={self.ticket}"

        response = self.session.get(url)
        responseJSON = response.json()

        if response.status_code == 200:
            if responseJSON.get("error", "") == "":
                return responseJSON["url"]
            elif responseJSON.get("error") == "Invalid download ticket":
                raise InvalidDownloadTicket(response.url, self.session.cookies)
            else:
                raise Exception(f"getdownloadurl error: {responseJSON.get('error')}")
        else:
            raise requests.exceptions.HTTPError(response)

    @classmethod
    def __initDownload(self):
        """
        Replica _nonSubDl() de local.js:
          1. GET initDownload -> obtiene tsrdlticket cookie + data.url con el ticket
          2. Extrae el ticket de data.url (no hace falta navegar a esa página)
        Sin ningún wait: el timer de 4s existe SOLO en el JS del browser.
        """
        logger.info(f"Getting 'tsrdlticket' cookie for: {self.url.url}")

        response = self.session.get(
            f"https://www.thesimsresource.com/ajax.php"
            f"?c=downloads&a=initDownload"
            f"&itemid={self.url.itemId}&setItems=&format=zip"
        )
        data = response.json()

        if data.get("error", ""):
            raise Exception(f"initDownload error: {data['error']}")

        # data["url"] == "/downloads/download/itemId/1775066/ticket/tsr69c7..."
        # Extraemos el ticket para pasarlo directo a getdownloadurl
        redirect_url = data.get("url", "")
        ticket_match = re.search(r"/ticket/([^/]+)$", redirect_url)
        if ticket_match:
            self.ticket = ticket_match.group(1)
            logger.debug(f"Got ticket: {self.ticket}")
        else:
            logger.warning(f"No se pudo extraer ticket de: {redirect_url}")

        # Asegurarse que tsrdlticket quede en la sesión
        tsrdlticket = response.cookies.get("tsrdlticket") or self.session.cookies.get("tsrdlticket")
        if tsrdlticket:
            self.session.cookies.set("tsrdlticket", tsrdlticket, domain=".thesimsresource.com")
            logger.debug(f"Got tsrdlticket: {tsrdlticket}")