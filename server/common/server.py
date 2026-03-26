import socket
import logging
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

    def stop(self):
        logging.info("action: stop_server | result: in_progress")
        self._is_running = False
        self._server_socket.close()
        logging.info("action: close_server_socket | result: success")

    def run(self):
        """
        Phase 1: Accept connections, receive batches and DONE messages.
        Phase 2: Once all agencies sent DONE, perform the sorteo.
        Phase 3: Answer any pending REQUEST_WINNERS and wait for remaining REQUEST_WINNERS.
        """
        pending_clients = []
        finished_agencies = set()
        served_agencies = set()

        # Phase 1: receive bets and wait for all agencies to send DONE
        while self._is_running and len(finished_agencies) < self._agencies:
            client_sock = self.__accept_new_connection()
            if client_sock is None:
                continue

            self.__handle_client_connection(
                client_sock, pending_clients, finished_agencies
            )

        if not self._is_running:
            for sock, _ in pending_clients:
                sock.close()
            return

        # Phase 2: sorteo
        logging.info("action: sorteo | result: success")
        all_bets = list(utils.load_bets())

        # Send to those who already requested winners
        for sock, agency_id in pending_clients:
            self.__send_winners_to_agency(sock, agency_id, all_bets, served_agencies)

        pending_clients.clear()

        # Phase 3: Wait for those who haven't requested winners yet
        while self._is_running and len(served_agencies) < self._agencies:
            client_sock = self.__accept_new_connection()
            if client_sock is None:
                continue

            self.__handle_request_winners_connection(
                client_sock, all_bets, served_agencies
            )

    def __handle_request_winners_connection(
        self, client_sock, all_bets, served_agencies
    ):
        """
        Reads exactly one message from the socket, verifies it is a request_winners
        message, and sends the winners to the agency.
        """
        try:
            msg_type, agency_id, _ = protocol.recv_message(client_sock)
            if msg_type == "request_winners":
                self.__send_winners_to_agency(
                    client_sock, agency_id, all_bets, served_agencies
                )
            else:
                # Should not occur, close connection
                protocol.send_error(client_sock)
                client_sock.close()
        except Exception as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            client_sock.close()

    def __send_winners_to_agency(self, sock, agency_id, all_bets, served_agencies):
        try:
            winners = [
                bet.document
                for bet in all_bets
                if bet.agency == agency_id and utils.has_won(bet)
            ]
            protocol.send_winners(sock, winners)
        except Exception as e:
            logging.error(
                f"action: send_winners | result: fail | agency: {agency_id} | error: {e}"
            )
        finally:
            sock.close()
            served_agencies.add(agency_id)

    def __handle_client_connection(
        self, client_sock, pending_clients, finished_agencies
    ):
        """
        Reads messages from the socket until the client disconnects or sends a DONE,
        unless it's a request_winners message, in which case it keeps it open.
        """
        try:
            while True:
                try:
                    msg_type, agency_id, payload = protocol.recv_message(client_sock)
                except ConnectionError:
                    # El cliente cerró la conexión
                    client_sock.close()
                    break

                if msg_type == "batch":
                    self.__process_batch(agency_id, payload)
                    protocol.send_ack(client_sock)

                elif msg_type == "done":
                    finished_agencies.add(agency_id)
                    protocol.send_ack(client_sock)
                    client_sock.close()
                    break

                elif msg_type == "request_winners":
                    pending_clients.append((client_sock, agency_id))
                    break

        except protocol.BetFormatError as e:
            logging.error(
                f"action: apuesta_recibida | result: fail | cantidad: {e.batch_size}"
            )
            protocol.send_error(client_sock)
            client_sock.close()

        except Exception as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            client_sock.close()

    def __process_batch(self, agency_id, payload):
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

        utils.store_bets(bets_to_store)
        logging.info(
            f"action: apuesta_recibida | result: success | cantidad: {len(payload)}"
        )

    def __accept_new_connection(self):
        """
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
