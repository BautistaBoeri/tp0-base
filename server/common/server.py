import socket
import logging
import threading
from common import protocol
from common import utils


class Server:
    def __init__(self, port, listen_backlog, agencies):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._is_running = True
        self._agencies = agencies
        self._store_lock = threading.Lock()
        
        # Sincronización para el sorteo sincronizado por el main thread
        self._agencias_recibidas = 0
        self._agencias_recibidas_lock = threading.Lock()
        # Evento que el hilo principal activa cuando termina el sorteo
        self._sorteo_terminado_event = threading.Event()
        # Evento para avisarle al hilo principal que ya reibimos todo de todos
        self._todos_listos_event = threading.Event()

    def stop(self):
        logging.info('action: stop_server | result: in_progress')
        self._is_running = False
        self._sorteo_terminado_event.set()
        self._todos_listos_event.set()
        self._server_socket.close()
        logging.info('action: close_server_socket | result: success')

    def run(self):
        """
        Create a thread for each agency connection.
        El hilo principal esperará a que todos reciban bets, luego calculará el sorteo
        en una variable y avisará a los hilos trabajadores para que la lean de forma segura.
        """
        threads = []
        clientes_aceptados = 0

        # Recibo apuestas de los clientes en hilos separados
        while self._is_running and clientes_aceptados < self._agencies:
            client_sock = self.__accept_new_connection()
            if client_sock is None:
                continue

            t = threading.Thread(target=self.__receive_worker, args=(client_sock,))
            t.start()
            threads.append(t)
            clientes_aceptados += 1

        # El hilo principal espera a que todos los trabajadores hayan terminado de guardar las bets 
        # y hayan entrado en estado de "request_winners"
        if self._is_running:
            self._todos_listos_event.wait()
            
            # Sorteo de todas las apuestas, lo hace solo el hilo principal una vez
            logging.info('action: sorteo | result: success')
            self._all_bets = list(utils.load_bets())
            
            # Da la señal a los worker de que ya pueden despertar, leer el vector y avisar a clientes
            self._sorteo_terminado_event.set()

        for t in threads:
            t.join()

    def __receive_worker(self, client_sock):
        """
        Worker thread function to handle receiving bets from a single client,
        wait for all others, and then send winners.
        """
        try:
            agency_id = None
            while True:
                msg_type, msg_agency_id, payload = protocol.recv_message(client_sock)
                agency_id = msg_agency_id

                if msg_type == 'batch':
                    bets_to_store = [
                        utils.Bet(
                            str(agency_id),
                            bet.first_name,
                            bet.last_name,
                            bet.document,
                            bet.birthdate,
                            bet.number
                        )
                        for bet in payload
                    ]
                    with self._store_lock:
                        utils.store_bets(bets_to_store)
                    logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(payload)}')
                    protocol.send_ack(client_sock)

                elif msg_type == 'done':
                        self._agencias_recibidas += 1
                        if self._agencias_recibidas == self._agencies:
                            self._todos_listos_event.set()

                elif msg_type == 'request_winners':

                    self._sorteo_terminado_event.wait()
                    
                    if not self._is_running:
                        break 

                    winners = [
                        bet.document
                        for bet in self._all_bets
                        if bet.agency == agency_id and utils.has_won(bet)
                    ]
                    protocol.send_winners(client_sock, winners)
                    break
        except protocol.BetFormatError as e:
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: {e.batch_size}')
            protocol.send_error(client_sock)
        except Exception as e:
            logging.error(f"action: receive_message_or_wait | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        logging.info('action: accept_connections | result: in_progress')
        try:
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._is_running:
                raise e
            return None
