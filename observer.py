import Pyro5.api  # Importa a API Pyro5 para comunicação entre processos
import os
import threading
import time

# Limpar o terminal
os.system('cls' if os.name == 'nt' else 'clear')

class Observer:
    def __init__(self):
        self.temp = []  # Armazena o dado recém-chegado
        self.log = []   # Lista para armazenar os dados replicados
        self.epoch = 1  # Época atual
        self.heartbeat_interval = 5  # Intervalo de envio de heartbeat (em segundos)
        self.lock = threading.Lock()  # Lock para gerenciar o estado de forma thread-safe

    def send_heartbeat(self):
        # Método para enviar o heartbeat para o líder
        ns = Pyro5.api.locate_ns()                          # Localiza o servidor de nomes Pyro
        leader = Pyro5.api.Proxy(ns.lookup("Lider-Epoca1"))  # Obtém o proxy do líder
        try:
            response = leader.receive_heartbeat(self.uri)  # Envia o heartbeat para o líder
            print(f"[Votante] Heartbeat enviado para o líder.")
        except Exception as e:
            print(f"[Votante] Erro ao enviar heartbeat: {e}")

    def request_state(self):
        ns = Pyro5.api.locate_ns()
        leader = Pyro5.api.Proxy(ns.lookup("Lider-Epoca1"))
        epoch, temp, log = leader.get_state()
        [self.epoch, self.temp, self.log] = [epoch, temp, log]
        print(f"[Votante] Estado requisitado do líder: log={self.log}, temp={self.temp}, epoch={self.epoch}")

    def start_heartbeat(self):
        # Método para iniciar o envio contínuo de heartbeats
        while True:
            self.send_heartbeat()  # Envia o heartbeat
            time.sleep(self.heartbeat_interval)  # Espera pelo intervalo antes de enviar novamente

    @Pyro5.api.expose  # Expondo a classe para ser acessada remotamente
    def replicate(self):
        # Método para replicar dados recebidos do líder
        ns = Pyro5.api.locate_ns()                          # Localiza o servidor de nomes Pyro
        leader = Pyro5.api.Proxy(ns.lookup("Lider-Epoca1"))  # Obtém o proxy do líder

        print(f"[Votante] Log temporário: {self.temp}")
        print(f"[Votante] Log commitado: {self.log}")

        response = leader.get_data(self.epoch, len(self.log))

        if response["data"] == "offset_fault":
            response = {"error": "offset"}

        while "error" in response:
            if response["error"] == "epoch":
                print("[Votante] Época diferente, excluindo logs.")
                self.temp = []
                self.log = []
                self.epoch = response["epoch"]
            elif response["error"] == "offset":
                print("[Votante] Logs diferentes, solicitando reenvio.")
                self.request_state()
                self.temp.pop()
            response = leader.get_data(self.epoch, len(self.log))
            
        self.temp.append(response["data"])
        self.epoch = response["epoch"]
        leader.confirm_data(self.epoch, len(self.log))
        print(f"[Votante] Dados replicados: {response['data']}")
    
    @Pyro5.api.expose
    def commit_log(self, offset):
        self.log.append(self.temp[offset])
        print(f"[Votante] Log temporário: {self.temp}")
        print(f"[Votante] Log commitado: {self.log}")

    @Pyro5.api.expose
    def update_log(self, data):
        """Método para atualizar o log de observadores"""
        self.temp.append(data)
        self.log.append(data)

if __name__ == "__main__":
    daemon = Pyro5.api.Daemon()                         # Cria o daemon Pyro para gerenciar a comunicação
    ns = Pyro5.api.locate_ns()                          # Localiza o servidor de nomes Pyro
    Observer1 = Observer()                                   # Cria a instância do broker votante 1
    uri = daemon.register(Observer1)                       # Registra o objeto votante no daemon
    leader = Pyro5.api.Proxy(ns.lookup("Lider-Epoca1")) # Obtém o proxy do líder
    Observer1.uri = uri                                    # Armazena o URI do votante para enviar o heartbeat
    print(leader.register_broker(uri, "observador"))       # Registra o votante 1 no líder com o estado "votante"
    Observer1.request_state()                              # Requisita o estado atual do líder
    print("Votante 1 pronto.")                          # Exibe no terminal que o broker votante 1 está pronto
    
    # Inicia a thread para enviar heartbeats de forma contínua
    heartbeat_thread = threading.Thread(target=Observer1.start_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    daemon.requestLoop()                                # Entra no loop de espera para atender as requisições do líder
