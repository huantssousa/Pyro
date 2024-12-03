import Pyro5.api
import os
import threading
import time

os.system('cls' if os.name == 'nt' else 'clear')

class Leader:
    def __init__(self):
        self.brokers = []       
        self.temp = []          
        self.log = []           # Log de dados enviados e replicados
        self.epoch = 1          
        self.heartbeat_timeout = 10  # Intervalo de tempo para esperar o heartbeat
        self.lock = threading.Lock()  # Lock para gerenciar a lista de brokers de forma thread-safe

    @Pyro5.api.expose  
    def register_broker(self, broker_uri, state):        
        with self.lock:
            self.brokers.append({"uri": broker_uri, "state": state, "last_heartbeat": time.time()})    
        print(f"Broker registrado: {broker_uri}, Estado: {state}")  
        return f"Broker {state} registrado com sucesso."            

    @Pyro5.api.expose
    def receive_heartbeat(self, broker_uri):
        with self.lock:
            for broker in self.brokers:
                if broker["uri"] == broker_uri:
                    broker["last_heartbeat"] = time.time()  
                    print(f"Heartbeat recebido de {broker_uri}")
                    return "Heartbeat registrado com sucesso."
        return "Erro: Broker não encontrado."

    # Leader
    def check_heartbeats(self):
        # Método para verificar os heartbeats dos votantes
        while True:
            with self.lock:
                current_time = time.time()
                for broker in self.brokers:
                    
                    if broker["state"] == "votante" and current_time - broker["last_heartbeat"] > self.heartbeat_timeout:
                        print(f"Votante {broker['uri']} falhou. Promovendo observador.")
                        # Promove o observador a votante
                        for b in self.brokers:
                            if b["state"] == "observador":
                                b["state"] = "votante"
                                b["last_heartbeat"] = current_time
                                print(f"Observador {b['uri']} agora é um votante.")
                                break
                        # Remove o votante falho
                        self.brokers = [b for b in self.brokers if b["uri"] != broker["uri"]]
                        break
            time.sleep(self.heartbeat_timeout)  # Espera o tempo do heartbeat antes de verificar novamente

    @Pyro5.api.expose
    def receive_data(self, data):
        # Método para o líder receber e replicar dados para os brokers votantes
        self.temp.append({"data": data, "confirmations": 0})  
        print(f"Líder recebeu e armazenou: {data}")           

        epoch = self.epoch
        offset = len(self.log)
        q_brokers = -(-len([b for b in self.brokers if b["state"] == "votante"]) // 2)

        for broker in self.brokers:
            if broker["state"] == "votante":                        
                try:
                    broker_proxy = Pyro5.api.Proxy(broker["uri"])   
                    broker_proxy.replicate()                        
                except:
                    self.brokers = [b for b in self.brokers if b["uri"] != broker["uri"]]
                    print(self.brokers)
                    print(f"Erro ao replicar para {broker['uri']}")

        if self.temp[offset]["confirmations"] >= q_brokers:  
            self.log.append(self.temp[offset]["data"])
            for broker in self.brokers:
                try:
                    if broker["state"] == "votante":
                        broker_proxy = Pyro5.api.Proxy(broker["uri"])
                        broker_proxy.commit_log(offset)

                    else:                                    # Se for observador, atualiza o log
                        broker_proxy = Pyro5.api.Proxy(broker["uri"])
                        broker_proxy.update_log(data)

                except Exception as e:
                    self.brokers = [b for b in self.brokers if b["uri"] != broker["uri"]]
                    print(self.brokers)
                    print(f"Erro ao replicar para {broker['uri']}, erro: {e}")                    

        return f"Dado '{data}' replicado para votantes."        # Retorna confirmação da replicação

    @Pyro5.api.expose
    def get_data(self, epoch, offset):
        if epoch != self.epoch:
            print(f"Época diferente, excluindo logs.")
            return {"error": "epoch", "epoch": self.epoch, "offset": len(self.log)}        # Esse erro não precisa tratar, já que a época não muda (segundo a prof)
        elif (epoch == self.epoch and offset != len(self.log)):
            print(f"Logs diferentes, solicitando reenvio.")
            return {"error": "offset", "epoch": self.epoch, "offset": len(self.log)}
        
        return {"epoch": self.epoch, "offset": offset, "data": self.temp[offset]["data"]}
    
    @Pyro5.api.expose
    def get_state(self):
        # Método para obter o estado do líder
        temp = [t["data"] for t in self.temp]
        return self.epoch, temp, self.log

    @Pyro5.api.expose
    def confirm_data(self, epoch, offset):
        self.temp[offset]["confirmations"] += 1
        print(f"Confirmação recebida para {epoch}, {offset}")
        return "Confirmação recebida."

    @Pyro5.api.expose
    def consume(self):
        # Método para consumir dados do log do líder
        if self.log:
            return self.log[-1]        
        return "Nenhum dado disponível."   

if __name__ == "__main__":
    daemon = Pyro5.api.Daemon()         # Cria o daemon Pyro para gerenciar a comunicação
    ns = Pyro5.api.locate_ns()          # Localiza o servidor de nomes Pyro
    leader = Leader()                   # Cria a instância do líder
    uri = daemon.register(leader)       # Registra o objeto líder no daemon
    ns.register("Lider-Epoca1", uri)    # Registra o URI do líder no serviço de nomes
    print("Líder pronto.")              # Exibe no terminal que o líder está pronto
    
    # Inicia uma thread para verificar os heartbeats
    heartbeat_thread = threading.Thread(target=leader.check_heartbeats)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    daemon.requestLoop()                
