import Pyro5.api
import os
import time
import threading

# Limpar o terminal
os.system('cls' if os.name == 'nt' else 'clear')

class Publisher:
    def __init__(self):
        self.log = []  # Lista para armazenar os dados publicados
        self.leader_proxy = None  # Inicialmente, sem o proxy do líder

    def connect_to_leader(self):
        try:
            # Conecta-se ao líder via serviço de nomes
            self.leader_proxy = Pyro5.api.Proxy(Pyro5.api.locate_ns().lookup("Lider-Epoca1"))
            print("[Publicador] Conectado ao líder.")
        except Exception as e:
            print(f"[Publicador] Erro ao conectar ao líder: {e}")

    def publish(self, data):
        if not self.leader_proxy:
            print("[Publicador] Não está conectado ao líder, tentando reconectar...")
            self.connect_to_leader()
        
        if self.leader_proxy:
            try:
                # Envia o dado para o líder
                response = self.leader_proxy.receive_data(data)
                print(f"[Publicador] Publicado: {data}, Resposta do líder: {response}")
            except Exception as e:
                print(f"[Publicador] Erro ao publicar dado: {e}")

    def check_leader_status(self):
        while True:
            if not self.leader_proxy:
                print("[Publicador] Tentando reconectar ao líder...")
                self.connect_to_leader()
            time.sleep(5)  # Verifica a cada 5 segundos

def main():
    print("Publicador pronto.")
    publisher = Publisher()  # Cria a instância do broker publicador
    publisher.connect_to_leader()  # Conecta-se ao líder no início

    # Inicia uma thread para monitorar a conexão com o líder
    threading.Thread(target=publisher.check_leader_status, daemon=True).start()

    while True:
        data = input("Digite o dado a ser publicado (ou 'exit' para sair): ")  # Solicita ao usuário um dado para publicar
        if data.lower() == "exit":
            break

        publisher.publish(data)  # Publica o dado no líder

if __name__ == "__main__":
    main()
