import socket
import logging
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

    def stop(self):
        logging.info('action: stop_server | result: in_progress')
        self._is_running = False
        self._server_socket.close()
        logging.info('action: close_server_socket | result: success')

    def run(self):
        """
        Phase 1: Accept connections, read one message.
        Phase 2: Once all agencies sent DONE and requested winners, perform the sorteo.
        Phase 3: Send each agency its winners and close sockets.
        """
        # List of (client_sock, agency_id) tuples
        pending_clients = []
        finished_agencies = set()

        # Phase 1: receive bets and wait for all agencies
        while self._is_running:
            if len(finished_agencies) == self._agencies and len(pending_clients) == self._agencies:
                break

            client_sock = self.__accept_new_connection()
            if client_sock is None:
                continue

            self.__handle_client_connection(client_sock, pending_clients, finished_agencies)

        if not self._is_running:
            for sock, _ in pending_clients:
                sock.close()
            return

        # Phase 2: sorteo
        logging.info('action: sorteo | result: success')
        all_bets = list(utils.load_bets())

        # Phase 3: send winners to each agency
        for client_sock, agency_id in pending_clients:
            try:
                winners = [
                    bet.document
                    for bet in all_bets
                    if bet.agency == agency_id and utils.has_won(bet)
                ]
                protocol.send_winners(client_sock, winners)
            except Exception as e:
                logging.error(f"action: send_winners | result: fail | agency: {agency_id} | error: {e}")
            finally:
                client_sock.close()

    def __handle_client_connection(self, client_sock, pending_clients, finished_agencies):
        """
        Reads exactly one message from the socket, processes it, and closes the socket
        unless it's a request_winners message, in which case it keeps it open.
        """
        try:
            msg_type, agency_id, payload = protocol.recv_message(client_sock)

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
                utils.store_bets(bets_to_store)
                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(payload)}')
                protocol.send_ack(client_sock)
                client_sock.close()

            elif msg_type == 'done':
                finished_agencies.add(agency_id)
                protocol.send_ack(client_sock)
                client_sock.close()

            elif msg_type == 'request_winners':
                pending_clients.append((client_sock, agency_id))

        except protocol.BetFormatError as e:
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: {e.batch_size}')
            protocol.send_error(client_sock)
            client_sock.close()

        except Exception as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
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
