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
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._is_running = True
        self._agencies = agencies
        self._store_lock = threading.Lock()
        
        # Thread Pool configuration
        self._pool_size = 5 
        self._task_queue = queue.Queue()
        self._workers = []

        # Sincronización para el sorteo liderado por hilo supervisor
        self._agencias_recibidas = 0
        self._agencias_recibidas_lock = threading.Lock()
        
        self._sorteo_terminado_event = threading.Event()
        self._todos_listos_event = threading.Event()

    def stop(self):
        logging.info('action: stop_server | result: in_progress')
        self._is_running = False
        
        # Destrabo a quienes esten bloqueados en los eventos
        self._todos_listos_event.set()
        self._sorteo_terminado_event.set()
   
        for _ in range(self._pool_size):
            self._task_queue.put(None)
            
        self._server_socket.close()
        logging.info('action: close_server_socket | result: success')

    def run(self):
        """
        Inicia threads del Pool fijos que consumiran de la queue y
        el main thread despacha los socket entrantes a esa cola.
        Tambien maneja el supervisor de sorteo.
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
            
            # Delega el socket a algun Worker libre poniendolo en la queue
            self._task_queue.put(client_sock)

        # Main thread solo va a joinear los workers (quedate tranquilo q ya pusimos self._is_running=False) 
        for t in self._workers:
            t.join()


    def __supervisor_sorteo(self):
        """Espera que lleguen todos los mensajes de Done y luego realiza sorteo."""
        self._todos_listos_event.wait()
        
        if self._is_running:
            logging.info('action: sorteo | result: success')
            self._all_bets = list(utils.load_bets())
            self._sorteo_terminado_event.set()


    def __worker_loop(self):
        """
        Toma una conexion de la queue, la procesa y la cierra. 
        Asi sucesivamente hasta el fin de los tiempos.
        """
        while self._is_running:
            client_sock = self._task_queue.get()
            
            # Si encolaron sentinela None, significa que apagaron Server. Salgo
            if client_sock is None:
                break
                
            self.__handle_client_connection(client_sock)
            

    def __handle_client_connection(self, client_sock):
        """
        Realiza exactamente un request/response del cliente. 
        En Golang creamos conexiones nuevas por cada peticion.
        """
        try:
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
                with self._agencias_recibidas_lock:
                    self._agencias_recibidas += 1
                    if self._agencias_recibidas == self._agencies:
                        self._todos_listos_event.set()
                        
                protocol.send_ack(client_sock)

            elif msg_type == 'request_winners':
                # El worker se clava aca hasta que corra el sorteo el supervisor
                self._sorteo_terminado_event.wait()
                
                if not self._is_running:
                    return

                winners = [
                    bet.document
                    for bet in self._all_bets
                    if bet.agency == agency_id and utils.has_won(bet)
                ]
                protocol.send_winners(client_sock, winners)

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
