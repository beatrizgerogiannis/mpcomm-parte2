# MPComm - Multiparty Communication

## Execução local com Docker
Requer Docker e Docker Compose v2. Sobe 1 GroupMngr, 1 comparisonServer e N peers
(`N_PEERS`, padrão 6) numa rede bridge local. Os logs dos peers aparecem em `./logs`.

Modo automático (teste rápido; `AUTO_ROUNDS` aceita uma lista, ex.: `20,50`):
```bash
rm -rf logs && mkdir logs
AUTO_ROUNDS=20 timeout 180 docker compose up --build --exit-code-from comparison
echo "código de saída: $?"
for f in logs/logfile*.log; do python3 -c "import ast,sys; print(sys.argv[1], len(ast.literal_eval(open(sys.argv[1]).read())))" "$f"; done
docker compose down
```

Modo interativo (como no original):
```bash
docker compose up --build -d groupmngr peer
docker compose run --rm --use-aliases comparison
# digite 20; depois 0 para encerrar
docker compose down
```

Para outro número de peers: `N_PEERS=8 AUTO_ROUNDS=20 docker compose up --build --exit-code-from comparison`.
Em rede local o número de "unordered message rounds" costuma ser 0 ou baixo; isso é esperado.

## Sockets e XML-RPC

As interações cliente-servidor (GroupMngr, sinal de início e envio do log) usam XML-RPC da biblioteca padrão. O multicast entre peers continua em UDP. A comparação abaixo é com a tag `sockets-baseline`.

O `peerCommunicatorUDP.py` inteiro passou de 201 para 395 linhas. A maior parte desse aumento é a entrega confiável e o FIFO, que não entram nesta comparação. Os números de linhas abaixo cobrem só o cliente-servidor: o `GroupMngr.py` inteiro, o `comparisonServer.py` sem as 65 linhas de `check_fifo_and_completeness`, e no peer o trecho que registra, lista, recebe o início da rodada e devolve o log.

| | `sockets-baseline` | XML-RPC |
|---|---:|---:|
| Linhas, GroupMngr.py | 39 | 104 |
| Linhas, comparisonServer.py sem a checagem FIFO | 142 | 170 |
| Linhas, trecho cliente-servidor do peer | 57 | 81 |
| `register` | 64 B | 210 B |
| resposta de `list` com 6 IPs | 94 B | 396 B |
| `start(0, 20)` | 18 B | 190 B |
| log de 120 mensagens (6×20) | 736 B | 12 415 B |
| log de 3000 mensagens (6×500) | 20 764 B | 311 055 B |
| serializar o log de 3000, nesta máquina | 0,22 ms | 3,26 ms |
| ida e volta local do log de 3000 | 0,16 ms | 21,8 ms |

Os tamanhos do lado socket são o `pickle` da mensagem. Os do XML-RPC são o XML do pedido ou da resposta, com o nome do método. A ida e volta do socket leu o `pickle` inteiro; a do XML-RPC foi uma chamada `submitLog` de verdade, com as 3000 mensagens entregues.

**Vantagens do XML-RPC neste código**

- O corpo HTTP traz o tamanho. Some o `recv` de tamanho fixo (`2048` no GroupMngr, `512` na confirmação, `1024` no início, `32768` no log). Um `recv(32768)` também só funciona se o TCP entregar a mensagem inteira numa leitura. O `pickle` de 3000 mensagens tem 20 764 B e ainda cabe nesse buffer; o estouro medido fica por volta de 4800 mensagens no log (cerca de `nMsgs=800` com 6 peers). A rodada Docker com `nMsgs=500` entregou 3000 mensagens em cada peer.
- `register` devolve confirmação e é idempotente por IP. Reiniciar um peer não cria outra entrada: o GroupMngr registrou os 6, o reinício do mesmo contêiner gerou `Peer already registered` e `list()` continuou com 6 IPs distintos.
- `unregister` existe e também é idempotente. No baseline era `pass`.
- Falha de método vira `xmlrpc.client.Fault` ou `ProtocolError`, além de `OSError` quando a conexão não abre. O peer repete o `register` nesses dois casos.

**Desvantagens**

- A mensagem fica maior: cerca de 3 a 4 vezes no `register` e no `list`, 10 vezes no `start`, 15 a 17 vezes no log. O log de 3000 mensagens sai de 20 KB para 311 KB.
- A chamada é mais lenta. Na mesma máquina, a ida e volta desse log passou de 0,16 ms para 21,8 ms. No experimento isso fica pequeno perto da espera aleatória de 10 a 100 ms entre mensagens UDP.
- XML-RPC não tem tupla. O log chega como lista de listas e `restore_log` reconstitui `(remetente, sequência)` antes da comparação.
- O servidor atende uma chamada por vez. O comparison precisa de fila (`max(N, 16)`) para os logs que chegam juntos.
- Os dois lados continuam acoplados pelo nome dos métodos e pela porta em `constMP.py`. O que saiu foi o acordo implícito sobre as chaves do dicionário `op` e sobre o tamanho de cada `recv`.

## Evidência com perda e atraso

O sistema completo (XML-RPC no cliente-servidor, entrega confiável e FIFO no UDP) foi executado no Docker com `tc netem` só no UDP da porta 6789. Seis peers, duas rodadas de 20 mensagens, commit `13e0bc0`. A saída integral está em:

- [`docs/evidencias/rpc-fifo-loss10.txt`](docs/evidencias/rpc-fifo-loss10.txt) — `NETEM="loss 10%"`
- [`docs/evidencias/rpc-fifo-delay20-loss10.txt`](docs/evidencias/rpc-fifo-delay20-loss10.txt) — `NETEM="delay 20ms 10ms loss 10%"`

Nos dois cenários o comparison saiu com código 0. Cada log tinha 120 mensagens, a checagem de FIFO passou nos seis peers nas duas rodadas, e `tests/check_logs.py` terminou com OK. Houve retransmissões, duplicatas descartadas e mensagens retidas na fila de espera, como esperado com perda. A métrica de ordem total acusou 117 a 120 rodadas fora de ordem; isso não é falha de FIFO. Não houve linha `ERROR` nem `WARNING`. O teste em nuvem não foi feito.

Demo of multicast among peers. Client-server calls use XML-RPC. The UDP multicast is reliable and delivers each sender's messages in FIFO order. The comparison server still reports a total-order metric: with loss or delay, that count stays high even when FIFO holds. The runs above are the evidence.

## Overall structure
A set of **peer processes** runs *peerCommunicatorUDP.py*. Each peer multicasts a sequence of messages at random intervals. Every message carries the sender id and a local sequence number. A long-lived thread receives UDP datagrams, acknowledges them and holds a message back until the earlier ones from that sender have been delivered. Another thread retransmits what has not been acknowledged. The main thread registers the peer, waits for the start signal, sends the round and submits the log.

*comparisonServer.py* asks the group manager for the peer list, calls `start(peerNumber, nMsgs)` on each peer, then receives each log with `submitLog`. It checks two things: FIFO and completeness per sender, and the original total-order count (`unordered message rounds`).

*GroupMngr.py* registers peers and lists them. `register` is idempotent per IP. `unregister` removes a peer. Both servers and the peers talk to each other through XML-RPC on the same ports as the old TCP sockets. Peer-to-peer traffic stays on UDP port 6789.

Loss and delay on that UDP port are simulated with `NETEM` (tc netem in Docker) or with `LOSS_RATE`, `DELAY_MS` and `JITTER_MS` (netsim.py). The local Docker run is the one used for the experiments. A cloud deployment is optional.

## Setup
Local execution is the Docker section at the top. For a manual run, set `SERVER_ADDR`, `GROUPMNGR_ADDR` and `N_PEERS` (or edit the defaults in *constMP.py*), start *GroupMngr.py*, then one *peerCommunicatorUDP.py* per peer, then *comparisonServer.py*.
