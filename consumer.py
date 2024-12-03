import Pyro5.api
import time
import threading

class Consumer:
    def __init__(self):
        self.log = []  # Lista para armazenar os dados consumidos
        self.leader_proxy = None  # Inicialmente, sem o proxy do líder

    def connect_to_leader(self):
        try:
            # Conecta-se ao líder via serviço de nomes
            self.leader_proxy = Pyro5.api.Proxy(Pyro5.api.locate_ns().lookup("Lider-Epoca1"))
            print("[Consumidor] Conectado ao líder.")
        except Exception as e:
            print(f"[Consumidor] Erro ao conectar ao líder: {e}")

    def consume(self):
        if not self.leader_proxy:
            print("[Consumidor] Não está conectado ao líder, tentando reconectar...")
            self.connect_to_leader()

        if self.leader_proxy:
            try:
                # Solicita dados ao líder
                data = self.leader_proxy.consume()
                if data != "Nenhum dado disponível.":
                    self.log.append(data)  # Adiciona o dado ao log
                    print(f"[Consumidor] Consumido: {data}")  # Exibe os dados consumidos
                else:
                    print("[Consumidor] Nenhum dado disponível.")
            except Exception as e:
                print(f"[Consumidor] Erro ao consumir dado: {e}")

    def check_leader_status(self):
        while True:
            if not self.leader_proxy:
                print("[Consumidor] Tentando reconectar ao líder...")
                self.connect_to_leader()
            time.sleep(5)  # Verifica a cada 5 segundos

def main():
    print("Consumidor pronto.")
    consumer = Consumer()  # Cria a instância do consumidor
    consumer.connect_to_leader()  # Conecta-se ao líder no início

    # Inicia uma thread para monitorar a conexão com o líder
    threading.Thread(target=consumer.check_leader_status, daemon=True).start()

    while True:
        consumer.consume()  # Consome dados do líder
        time.sleep(2)  # Aguarda 2 segundos entre as tentativas de consumo (ajustável)

if __name__ == "__main__":
    main()
