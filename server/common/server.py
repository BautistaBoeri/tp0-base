import socket
import logging
import threading
import queue
from common import protocol
from common import utils


class Server:
    def __init__(self, port, listen_backlog, agencies):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._is_running = True
        self._agencies = agencies
        self._store_lock = threading.Lock()

        # Thread Pool configuration
        self._pool_size = agencies
        self._task_queue = queue.Queue()
        self._workers = []

        # Sincronización para el sorteo liderado por hilo supervisor
        self._agencias_recibidas = 0
        self._agencias_recibidas_lock = threading.Lock()

        self._sorteo_terminado_event = threading.Event()
        self._todos_listos_event = threading.Event()

    def stop(self):
        logging.info("action: stop_server | result: in_progress")
        self._is_running = False

        # Desbloquea hilos esperando en los eventos de sincronización
        self._todos_listos_event.set()
        self._sorteo_terminado_event.set()

        for _ in range(self._pool_size):
            self._task_queue.put(None)

        self._server_socket.close()
        logging.info("action: close_server_socket | result: success")

    def run(self):
        """
        Inicia threads del Pool fijos que consumirán de la queue,
        mientras el main thread despacha los sockets entrantes a esta cola.
        También maneja el hilo supervisor del sorteo.
        """

        # Hilo supervisor que espera silenciosamente hasta que todas las agencias terminen para sortear
        sorteo_thread = threading.Thread(target=self.__supervisor_sorteo, daemon=True)
        sorteo_thread.start()

        # Iniciar Pool de Workers fijos
        for _ in range(self._pool_size):
            t = threading.Thread(target=self.__worker_loop)
            t.start()
            self._workers.append(t)

        # Loop principal: recibe requests y los mete en la queue
        while self._is_running:
            client_sock = self.__accept_new_connection()
            if client_sock is None:
                continue

            # Enruta la conexión entrante a un worker disponible
            self._task_queue.put(client_sock)

        for t in self._workers:
            t.join()

    def __supervisor_sorteo(self):
        """Espera que lleguen todos los mensajes de Done y luego realiza sorteo."""
        self._todos_listos_event.wait()

        if self._is_running:
            logging.info("action: sorteo | result: success")
            self._all_bets = list(utils.load_bets())
            self._sorteo_terminado_event.set()

    def __worker_loop(self):
        """
        Toma una conexión de la queue, la procesa por completo (incluso si envía 
        múltiples mensajes) y la cierra cuando el cliente se desconecta o termina.
        Se ejecuta indefinidamente hasta recibir un centinela None.
        """
        while self._is_running:
            client_sock = self._task_queue.get()

            # El centinela None significa que el servidor se está apagando
            if client_sock is None:
                break

            self.__handle_client_connection(client_sock)

    def __handle_client_connection(self, client_sock):
        """Procesa todas las solicitudes del cliente en una misma conexión."""
        try:
            while True:
                try:
                    msg_type, msg_agency_id, payload = protocol.recv_message(client_sock)
                except ConnectionError:
                    break

                if msg_type == "batch":
                    self.__handle_batch_message(client_sock, msg_agency_id, payload)
                elif msg_type == "done":
                    # Mantenemos la conexión abierta para enviar el ACK o esperar a pedir resultados si quisieran mandar algo por acá.
                    # Asumiendo que el cliente manda DONE y el servidor debe responder un ACK.
                    self.__handle_done_message(client_sock)
                elif msg_type == "request_winners":
                    self.__handle_request_winners_message(client_sock, msg_agency_id)
                    break # El protocolo asume que request_winners es lo último y cierra

        except protocol.BetFormatError as e:
            logging.error(
                f"action: apuesta_recibida | result: fail | cantidad: {e.batch_size}"
            )
            protocol.send_error(client_sock)
        except Exception as e:
            logging.error(
                f"action: receive_message_or_wait | result: fail | error: {e}"
            )
        finally:
            client_sock.close()

    def __handle_batch_message(self, client_sock, agency_id, payload):
        """Maneja la recepción y el almacenamiento de un lote de apuestas."""
        bets_to_store = []
        for dto in payload:
            bet = utils.Bet(
                str(agency_id),
                dto.first_name,
                dto.last_name,
                dto.document,
                dto.birthdate,
                dto.number,
            )
            bets_to_store.append(bet)

        with self._store_lock:
            utils.store_bets(bets_to_store)

        logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(payload)}")
        protocol.send_ack(client_sock)

    def __handle_done_message(self, client_sock):
        """Maneja el aviso de finalización de una agencia y actualiza los eventos de sincronización."""
        with self._agencias_recibidas_lock:
            self._agencias_recibidas += 1
            if self._agencias_recibidas == self._agencies:
                self._todos_listos_event.set()

        protocol.send_ack(client_sock)

    def __handle_request_winners_message(self, client_sock, agency_id):
        """Bloquea la ejecución hasta que finalice el sorteo, luego envía los ganadores a la agencia."""
        # Espera a que el hilo supervisor termine el proceso del sorteo
        self._sorteo_terminado_event.wait()

        if not self._is_running:
            return

        winners = [
            bet.document
            for bet in self._all_bets
            if bet.agency == agency_id and utils.has_won(bet)
        ]
        protocol.send_winners(client_sock, winners)

    def __accept_new_connection(self):
        """protocol.
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        logging.info("action: accept_connections | result: in_progress")
        try:
            c, addr = self._server_socket.accept()
            logging.info(
                f"action: accept_connections | result: success | ip: {addr[0]}"
            )
            return c
        except OSError as e:
            if self._is_running:
                raise e
            return None
