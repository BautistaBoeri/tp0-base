import sys

def main():
    if len(sys.argv) != 3:
        print("Uso: python3 mi-generador.py <archivo_salida> <cantidad_clientes>")
        sys.exit(1)

    output_file = sys.argv[1]
    
    try:
        num_clients = int(sys.argv[2])
    except ValueError:
        print("La cantidad de clientes debe ser un número entero.")
        sys.exit(1)

    yaml_header = """name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
"""

    yaml_clients = ""
    for i in range(1, num_clients + 1):
        yaml_clients += f"""
  client{i}:
    container_name: client{i}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={i}
      - CLI_NOMBRE=Santiago Lionel
      - CLI_APELLIDO=Lorca
      - CLI_DOCUMENTO=30904465
      - CLI_NACIMIENTO=1999-03-17
      - CLI_NUMERO=7574
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.yaml:/config.yaml
"""

    yaml_footer = """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

    try:
        with open(output_file, 'w') as f:
            f.write(yaml_header + yaml_clients + yaml_footer)
        print(f"Archivo {output_file} generado correctamente para {num_clients} clientes.")
    except Exception as e:
        print(f"Error escribiendo el archivo: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
