import socket
import logging
from common import protocol
from common import utils


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._is_running = True

    def stop(self):
        logging.info('action: stop_server | result: in_progress')
        self._is_running = False
        self._server_socket.close()
        logging.info('action: close_server_socket | result: success')
    
    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while self._is_running:
            client_sock = self.__accept_new_connection()
            if client_sock is not None:
                self.__handle_client_connection(client_sock)

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            while True:
                agency_id, dtos = protocol.recv_bet_batch(client_sock)
                if dtos is None:
                    break
                
                bets_to_store = []
                for dto in dtos:
                    bet_to_store = utils.Bet(agency_id, dto.first_name, dto.last_name, dto.document, dto.birthdate, dto.number)
                    bets_to_store.append(bet_to_store)
                
                utils.store_bets(bets_to_store)
                
                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(dtos)}')
                protocol.send_ack(client_sock)

        except protocol.BetFormatError as e:
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: {e.batch_size}')
            protocol.send_error(client_sock)

        except Exception as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        try:
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._is_running:
                raise e
            return None
