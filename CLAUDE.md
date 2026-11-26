Trabalho de Sistemas Distribuídos (UFG) — Parte 2

Repositório da Parte 2, partindo do estado final da Parte 1 (tag parte1-final). Base: MPComm do prof. Fábio (https://github.com/professorfabio/MPComm), multicast em Python 3.

Escopo da Parte 2 (entrega: domingo 06/12/2026, 23:59; código congela em 04/12)
Dar significado às mensagens: contas bancárias replicadas. Cada peer é uma réplica (máquina de estados replicada); as mensagens DATA carregam comandos (DEPOSITAR, SACAR, JUROS).
Tornar visível e mensurável que, só com FIFO por remetente, réplicas recebem comandos de remetentes diferentes em ordens diferentes e divergem (hash/diff do estado final).
Melhorar a arquitetura do código-base (camadas, JSON no lugar de pickle, ID estável, logs por rodada, ACK casado por ID, configuração 100% por variáveis de ambiente).
O sistema deve rodar em máquinas diferentes (nuvem), não só no Docker Compose local: nenhum IP/endereço fixo no código e nada que dependa do IP de origem de um pacote.
FORA DO ESCOPO: ordem total, ordem causal, sequenciador, relógios de Lamport, qualquer coordenação que "conserte" a divergência (são das Partes 3 e 4), e TRANSFERIR. Não implemente nada disso. A divergência é o resultado esperado desta parte.
Arquitetura alvo (camadas)
peer.py        orquestra a rodada: RPC start, gera comandos, aplica entregas, envia relatório
  ├─ bank.py       APLICAÇÃO: estado das contas, apply(cmd), state_hash()   (lógica pura)
  ├─ workload.py   APLICAÇÃO: gera os comandos de cada peer por rodada      (lógica pura)
  └─ multicast.py  ENTREGA: ReliableFifoMulticast (ACK, retransmissão, rodadas, hold-back)
       ├─ reliability.py  lógica pura do protocolo da Parte 1 (seq, ACK, hold-back FIFO)
       └─ transport.py    TRANSPORTE: socket UDP, encode/decode JSON, netsim

Interface de entrega (a Parte 3 troca a implementação sem mexer na aplicação): multicast(payload: dict) e callback on_deliver(sender: int, seq: int, payload: dict). A aplicação NUNCA importa socket, reliability ou transport.

Contratos (não mudar sem combinar com o grupo)
C1. Comandos (payload do DATA, JSON, valores sempre inteiros — nenhum float)
json
{"op": "DEPOSITAR", "conta": "A", "valor": 10000}   // centavos (R$ 100,00)
{"op": "SACAR",     "conta": "A", "valor": 50000}   // RECUSADO se saldo < valor
{"op": "JUROS",     "conta": "A", "bp": 100}        // pontos-base: 100 = 1%
Contas ACCOUNTS = ("A", "B"), saldo inicial 100000 cada; estado zerado a cada rodada.
JUROS: saldo = saldo + saldo * bp // 10000.
Bank.apply(cmd) retorna "OK", "RECUSADO" ou "INVALIDO"; nunca lança exceção.
Consulta é leitura local da réplica (não passa pelo multicast).
C2. Relatório da réplica (peer → comparisonServer, submitLog(report) via xmlrpc)
json
{"peer": 3,
 "log": [[0, 0], [2, 0], [0, 1]],
 "results": [[0, 0, "OK"], [2, 0, "RECUSADO"], [0, 1, "OK"]],
 "balances": {"A": "102010", "B": "95000"},
 "hash": "<sha256 hex>"}
balances como STRING: o xmlrpc só tem inteiro de 32 bits.
hash = sha256(json.dumps({conta: int(saldo)}, sort_keys=True, separators=(',', ':'))).
submitLog continua aceitando a lista antiga [[sender, seq], ...] (Métricas 1 e 2).
C3. Gerador de carga (workload.py)

generate(seed, rnd, peer_id, n, mode="random") -> list[dict]

random: random.Random(f"{seed}-{rnd}-{peer_id}"); 50% DEPOSITAR (1000–20000), 25% SACAR (5000–60000), 25% JUROS (bp 100–500), contas A/B.
tanenbaum: peer 0 → DEPOSITAR A 10000; peer 1 → JUROS A 100; demais → DEPOSITAR B 100.
C4. Configuração (nenhum IP/endereço fixo no código)
Obrigatórias: SERVER_ADDR, GROUPMNGR_ADDR (ausentes = None; o import de constMP não pode falhar; cada programa chama constMP.check_required(...) no início e sai com mensagem clara).
Opcionais: N_PEERS (6), SERVER_PORT (5678), PEER_TCP_PORT (5679), GROUPMNGR_TCP_PORT (5680), PEER_UDP_PORT (6789), PEER_ADVERTISE_IP (IP que o peer registra), PEER_IP_MODE (public|local).
Portas a liberar na nuvem: TCP 5678, 5679, 5680 e UDP 6789.
ACK é casado pelo ID de quem confirma (acker), nunca pelo IP de origem do pacote.
Mensagens UDP (JSON, listas com as mesmas posições da Parte 1)

["READY", round, sender] · ["DATA", round, sender, seq, payload] · ["END", round, sender, total] · ["ACK", round, acker, ackedKind, sender, seq]. Mensagem codificada > 1024 bytes é recusada no envio (limite do recvfrom).

Arquivos
constMP.py — constantes (portas, N, endereços dos servidores)
GroupMngr.py — registro e listagem de peers (xmlrpc); ID estável por peer (#8)
comparisonServer.py — coordena as rodadas; Métrica 1 (FIFO e perdas), Métrica 2 (ordem total, métrica antiga), Métrica 3 (consistência das réplicas, #7)
peerCommunicatorUDP.py — peer da Parte 1; será substituído por peer.py + multicast.py + transport.py
reliability.py — lógica pura do protocolo (não alterar o comportamento)
netconfig.py — IP que o peer anuncia (PEER_ADVERTISE_IP / PEER_IP_MODE) (#15)
netsim.py — simulação de perda/atraso no código (LOSS_RATE, DELAY_MS, JITTER_MS)
tests/ — unittest + check_logs.py; scripts/run_scenario.sh — cenário com Docker Compose
docs/evidencias/ — saídas salvas com o commit de origem no cabeçalho
CHANGES.md — registro de mudanças (vai para o README)
Como testar
Unitários: python -m unittest discover tests
Cenário: AUTO_ROUNDS=20,20 scripts/run_scenario.sh "delay 20ms 10ms loss 10%" (sai com 0 só se compose, check_logs e a busca por ERROR/WARNING passarem)
Divergência: WORKLOAD=tanenbaum AUTO_ROUNDS=1,1,1,1,1 scripts/run_scenario.sh "delay 50ms 40ms"
Ordem importa: sempre testar também com NETEM com atraso e jitter, senão nada diverge.
Regras de trabalho
Nenhuma operação de git (commit, push, branch, merge, tag). Sugira a mensagem de commit terminando com Closes #N; a Beatriz commita.
Leia os arquivos relevantes antes de editar. Não reescreva o que não foi pedido.
Uma tarefa (issue) por vez; cada tarefa termina com algo testável localmente (Docker Compose).
Lógica nova em módulos sem sockets, com testes unitários (como reliability.py).
Divergência entre réplicas é esperada: imprimir DIVERGENT, nunca ERROR/WARNING (o run_scenario.sh falha com essas palavras).
Nenhum IP, hostname ou endereço fixo no código: tudo vem de variável de ambiente (C4).
logs/logfile<ID>.log (lista de (sender, seq) da última rodada) precisa continuar existindo enquanto check_logs.py depender dele; logs por rodada são arquivos adicionais.
Não adicione dependências sem avisar e justificar (hoje: só requests).
Toda mudança de comportamento ou melhoria do código-base vai para CHANGES.md (o que era, por que era problema, o que mudou).
Mantenha nomes e mensagens de log consistentes com o idioma do arquivo editado.
Ganchos para a Parte 3: não acoplar a aplicação ao FIFO; IDs estáveis; payload genérico.
Estado atual (issues no GitHub; quadro "MPComm — Parte 2")
[ ] #1 Tag parte1-final + este CLAUDE.md (Beatriz)
[ ] #2 transport.py: JSON no lugar de pickle (Beatriz)
[ ] #3 multicast.py: ReliableFifoMulticast + interface de entrega (Beatriz)
[ ] #4 bank.py (Beatriz)
[ ] #5 peer.py: integra banco + workload, relatório C2, log por rodada (Beatriz)
[ ] #6 workload.py (Matheus)
[ ] #7 Métrica 3 no comparisonServer (Matheus)
[ ] #8 ID estável no GroupMngr (Geovanne)
[ ] #9 Modo interativo espera os N peers (Geovanne)
[ ] #10 Evidências da divergência (Matheus)
[ ] #11 README: aplicação, teoria, limitações — rascunho (Geovanne)
[ ] #12 ACK casado por ID — funções novas em reliability.py (Geovanne)
[ ] #13 README final + CHANGES.md (Beatriz)
[ ] #14 Checklist final (Beatriz)
[ ] #15 Configuração 100% por variáveis de ambiente: constMP.py + netconfig.py (Geovanne)
[ ] #16 Execução em máquinas diferentes: scripts docker run --network host + guia (Matheus)