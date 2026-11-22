# MPComm — Parte 1

Multicast entre peers em UDP, com as chamadas cliente-servidor em XML-RPC. Cada peer entrega as mensagens de um mesmo remetente em ordem FIFO e recupera o que se perde nesse UDP.

Integrantes e o que cada um cobre neste texto:

- **Beatriz Gerogiannis** — confiabilidade e FIFO
- **Geovanne** — simulação, verificação e a consolidação das seções abaixo
- **Matheus** — RPC e a comparação com sockets

O detalhe de cada parte está nas três seções finais. O registro passo a passo continua em [CHANGES.md](CHANGES.md).

## Funcionalidades

- O GroupMngr registra peers e devolve a lista de IPs. `register` é idempotente por IP: o mesmo peer que reinicia continua com uma entrada só. `unregister` tira o par IP e porta; um par que já saiu também é sucesso.
- O comparisonServer pede a lista, manda cada peer começar a rodada com `start(peerNumber, nMsgs)` e recebe o log com `submitLog`.
- Cada peer envia `nMsgs` mensagens a todos os outros, com intervalo aleatório de 10 a 100 ms. A mensagem de dados leva o remetente e um número de sequência local.
- O UDP entre peers confirma `READY`, `DATA` e `END`. O que fica sem ACK é retransmitido. Duplicata recebe outro ACK e não entra de novo no log.
- A entrega é FIFO por remetente: uma mensagem com sequência maior espera numa fila até chegar o que falta antes dela. A ordem entre remetentes diferentes continua livre.
- O comparisonServer confere duas coisas em cada rodada. A métrica 1 exige, para cada remetente, a sequência `0 .. nMsgs-1`, sem falta, sem duplicata e em ordem crescente. A métrica 2 conta posições em que os logs dos peers divergem (a ordem total do programa original). Com perda ou atraso a métrica 2 fica alta mesmo quando a métrica 1 passa.
- Perda e atraso no UDP saem por configuração: `NETEM` no Docker ou `LOSS_RATE`, `DELAY_MS` e `JITTER_MS` no `netsim.py`.

Ordem total, ordem causal e aplicação replicada ficam para as partes seguintes.

## Arquitetura

Três processos, nas mesmas portas do código original:

| Processo | Programa | Porta | Papel |
|---|---|---|---|
| GroupMngr | `GroupMngr.py` | 5680 | `register`, `list`, `unregister`, `stop` |
| comparisonServer | `comparisonServer.py` | 5678 | `start` nos peers e `submitLog` |
| peer | `peerCommunicatorUDP.py` | 5679 (RPC) e 6789 (UDP) | uma rodada e o multicast |

GroupMngr, comparisonServer e peer conversam por XML-RPC da biblioteca padrão (`xmlrpc.client` / `xmlrpc.server`). O multicast, o ACK e a retransmissão ficam em UDP na porta 6789, com `pickle`, como no programa de partida.

No Docker Compose há um serviço `groupmngr`, um `comparison` e N réplicas de `peer` (`N_PEERS`, padrão 6) na rede bridge `mpcomm`. Os endereços vêm de `SERVER_ADDR`, `GROUPMNGR_ADDR` e `N_PEERS` em [constMP.py](constMP.py). No Compose os nomes são `comparison` e `groupmngr`. Com `PEER_IP_MODE=local` o peer registra o IP do contêiner.

Cada peer tem três fluxos: a thread principal (registro, espera do `start`, envio da rodada e do log), uma thread receptora de vida longa e uma thread de retransmissão. A lógica de estado, sem socket, está em [reliability.py](reliability.py).

## Detalhes de implementação

**Cliente-servidor.** O GroupMngr atende um pedido por vez e responde ao `stop` antes de sair. A identidade do registro é o IP: a mesma porta mantém a entrada; outra porta substitui a anterior. `unregister(ip, porta)` só remove esse par, então um unregister atrasado de um processo antigo não apaga quem já se registrou de novo. O peer repete o `register` até 20 vezes em `OSError` e em `xmlrpc.client.Error`.

O `start` devolve `"Peer process <id> started."` e só então a rodada começa. Com `nMsgs == 0` o peer chama `unregister` dentro do `start`, antes da resposta, e o comparisonServer em seguida chama `stop` no GroupMngr. O modo `AUTO_ROUNDS` não envia esse 0 e não chama `stop`: a última rodada termina o comparisonServer com código 0.

XML-RPC não tem tupla. Cada entrada `(remetente, sequência)` chega como lista, e `restore_log` monta a tupla de novo antes das duas métricas. O comparisonServer abre o `listen` antes de chamar os `start`, com fila `max(N, 16)`, porque os peers submetem o log enquanto ele ainda está disparando os outros.

**UDP.** As mensagens são `('READY', rodada, remetente)`, `('DATA', rodada, remetente, seq)`, `('END', rodada, remetente, total)` e `('ACK', rodada, quem confirma, tipo, remetente, seq)`. `READY` e `END` usam `seq = -1` no ACK. A thread de retransmissão olha a cada 50 ms e reenvia o que está sem ACK há mais de `RETRANS_TIMEOUT_MS` (padrão 200). Depois de `MAX_RETRIES` (padrão 100) o peer registra `ERROR: giving up on ...` e segue. A rodada também tem teto de `ROUND_TIMEOUT_S` (padrão 120): no estouro o peer grava o log parcial e envia assim mesmo, para a verificação acusar a falta.

Uma mensagem de rodada futura não recebe ACK. O comparisonServer dá o sinal um peer por vez; se quem ainda está na rodada anterior confirmasse e descartasse, o remetente pararia de retransmitir e o `READY` da rodada nova se perderia. Mensagem de rodada passada recebe ACK e é descartada.

**FIFO.** `on_data` só entrega a sequência esperada daquele remetente. Uma sequência maior vai para a fila de espera e sai quando a lacuna fecha. O log gravado em `logs/logfile<ID>.log` é a ordem de entrega, uma lista de `(remetente, seq)`.

**Simulação.** `NETEM` aplica `tc netem` só ao UDP que sai com destino na porta 6789 (qdisc `prio`, netem na banda 1:3, filtro `u32`). ARP e o XML-RPC não passam por essa banda. `LOSS_RATE`, `DELAY_MS` e `JITTER_MS` fazem o mesmo efeito dentro de `netsim.send`, no lugar de `sendto`. Com os três em zero, `netsim` chama `sendto` direto.

## Como rodar

Docker e Docker Compose v2. Na raiz do repositório, modo automático (não pede teclado; `AUTO_ROUNDS` aceita uma lista):

```bash
rm -rf logs && mkdir logs
AUTO_ROUNDS=20 timeout 180 docker compose up --build --exit-code-from comparison
docker compose down
```

Modo interativo: `docker compose up --build -d groupmngr peer`, depois `docker compose run --rm --use-aliases comparison`. Digite o `nMsgs` (por exemplo 20) e, para encerrar, 0. Outro tamanho de grupo: `N_PEERS=8`.

Perda e atraso só no UDP dos peers:

```bash
NETEM="loss 10%" AUTO_ROUNDS=20,20 timeout 400 docker compose up --build --exit-code-from comparison
NETEM="delay 20ms 10ms loss 10%" AUTO_ROUNDS=20,20 timeout 400 docker compose up --build --exit-code-from comparison
```

A alternativa sem `tc`, inclusive fora do Linux:

```bash
LOSS_RATE=0.1 DELAY_MS=20 JITTER_MS=10 AUTO_ROUNDS=20,20 docker compose up --build --exit-code-from comparison
```

No PowerShell o mesmo efeito é `$env:AUTO_ROUNDS="20"; $env:NETEM="loss 10%"; docker compose up --build --exit-code-from comparison`.

Fora do Docker: defina `SERVER_ADDR`, `GROUPMNGR_ADDR` e `N_PEERS` (ou edite os padrões em `constMP.py`), suba `GroupMngr.py`, um `peerCommunicatorUDP.py` por peer e por último `comparisonServer.py`.

Testes, sem Docker:

```bash
python -m unittest discover -s tests -v
python tests/check_logs.py 6 20
```

`scripts/run_scenario.sh` sobe um cenário, roda `check_logs.py` e falha se aparecer `ERROR` ou `WARNING` em qualquer rodada. A saída de evidência deve ser gravada fora da árvore e movida depois para `docs/evidencias/`: um `tee` direto nesse diretório suja o repositório antes do cabeçalho anotar se a árvore estava limpa.

## Comparação sockets × RPC

Medido contra a tag `sockets-baseline` (`6c1dbd3`), neste código e nesta máquina. O `peerCommunicatorUDP.py` inteiro foi de 201 para 395 linhas; a maior parte é a entrega confiável e o FIFO, de fora desta tabela. As linhas abaixo são só o cliente-servidor: o `GroupMngr.py` inteiro, o `comparisonServer.py` sem as 65 linhas de `check_fifo_and_completeness`, e no peer o trecho que registra, lista, recebe o início e devolve o log.

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
| serializar o log de 3000 | 0,22 ms | 3,26 ms |
| ida e volta local do log de 3000 | 0,16 ms | 21,8 ms |

Os tamanhos do lado socket são o `pickle`. Os do XML-RPC são o XML do pedido ou da resposta, com o nome do método. A ida e volta do socket leu o `pickle` inteiro; a do XML-RPC foi uma chamada `submitLog` com as 3000 mensagens entregues.

O que se ganha neste código: o HTTP carrega o tamanho, e some o `recv` fixo (`2048` no GroupMngr, `512` na confirmação, `1024` no início, `32768` no log). Um `recv(32768)` ainda depende de o TCP entregar o recado numa leitura só. O `pickle` de 3000 mensagens tem 20 764 B e cabe nesse buffer; o corte medido fica por volta de 4800 mensagens no log (cerca de `nMsgs=800` com 6 peers). A rodada Docker com `nMsgs=500` entregou 3000 mensagens em cada um dos seis peers, com a métrica 1 em OK. `register` confirma e é idempotente: reiniciar um contêiner imprimiu `Peer already registered` e `list()` seguiu com 6 IPs. `unregister` existe. Falha de método vira `xmlrpc.client.Fault` ou `ProtocolError`, e o peer repete o registro.

O que se paga: a mensagem cresce (cerca de 3 a 4 vezes no `register` e no `list`, 10 vezes no `start`, 15 a 17 vezes no log; o log de 3000 sai de 20 KB para 311 KB). Na mesma máquina a ida e volta desse log passou de 0,16 ms para 21,8 ms, pouco perto da espera de 10 a 100 ms entre mensagens UDP. O log chega como lista de listas. O servidor atende uma chamada por vez, por isso a fila no comparisonServer. Os dois lados continuam acoplados pelo nome dos métodos e pela porta em `constMP.py`. O que saiu foi o acordo sobre as chaves do dicionário `op` e sobre o tamanho de cada `recv`.

## Problemas corrigidos

- Espera ocupada pelos handshakes e corrida do contador entre rodadas. A espera virou `Event`, e cada rodada tem o próprio estado.
- A comparação original parava antes da última posição e do último peer (`range` já exclui o limite). `count_unordered_rounds` percorre o log inteiro.
- `register` fazia `append` em todo mundo, e `unregister` era `pass`. Reiniciar um peer duplicava o IP; quem saía continuava na lista.
- `recv` de tamanho fixo no TCP truncava o log grande e o `pickle.loads` falhava. No cliente-servidor esse caminho agora é XML-RPC.
- `READY`, `DATA` e `END` se perdiam sem recuperação, e uma mensagem de dados que chegasse durante o handshake era descartada. ACK e retransmissão cobrem os três, e a thread receptora trata dados desde o início da rodada.
- O `tc netem` na raiz da interface derrubava também o ARP e o TCP com os dois servidores. O filtro ficou só no UDP 6789. Um `NETEM` inválido faz o contêiner sair com erro.
- As esperas de handshake e de fim de rodada não tinham prazo. Hoje há `ROUND_TIMEOUT_S`, com o que faltou impresso no log.
- Uma exceção na thread receptora ou na de retransmissão matava a thread em silêncio. Cada iteração captura, imprime o traceback e continua.
- Com perda, a retransmissão fazia o mesmo remetente entrar no log fora de ordem. A fila de espera segura a sequência maior até chegar a anterior. A evidência está em [docs/evidencias/fifo-antes.txt](docs/evidencias/fifo-antes.txt) (violações em todos os peers, `check_logs` FAILED) e [docs/evidencias/fifo-depois.txt](docs/evidencias/fifo-depois.txt) (0 violações, OK).
- O script de cenário podia terminar em OK com `ERROR` numa rodada que o `check_logs` já não via, porque cada rodada reescreve o `logfile`. `scripts/run_scenario.sh` procura `ERROR` e `WARNING` em todas as rodadas.

Ainda vale saber: o UDP entre peers continua em `pickle`. Desserializar dados da rede permite execução de código; ficou assim pela compatibilidade com o formato das mensagens. A métrica 2 continua contando divergência de ordem total. Nas rodadas com perda ela acusa por volta de 117 a 120 posições; a decisão de FIFO é a métrica 1.

## RPC e comparação

*Matheus.*

As quatro interações cliente-servidor saíram de TCP com `pickle` para XML-RPC, sem dependência nova: `register` e `list` no GroupMngr, `stop` do comparisonServer para o GroupMngr, `start(peerNumber, nMsgs)` do comparisonServer para o peer, e `submitLog` do peer para o comparisonServer. O multicast segue em UDP.

A escolha foi a biblioteca padrão, contra gRPC e Pyro5, para manter o programa em um arquivo por processo, nas mesmas portas, e para comparar com o socket sem instalar outro runtime. Os números dessa comparação estão na seção acima.

Dois cuidados do desenho: `start` responde antes da rodada, senão o handler XML-RPC ficaria ocupado até o `submitLog` e os dois lados esperariam um ao outro. O `unregister` do encerramento (`nMsgs == 0`) acontece dentro desse `start`, antes da resposta, para o GroupMngr receber o `stop` com o peer já fora da lista.

A rodada grande que o `recv(32768)` não garantia foi executada no Docker: `nMsgs=500`, seis peers, código 0, seis logs de 3000 mensagens, métrica 1 OK em todos (`0..499`). A métrica 2 acusou 80 posições diferentes entre os logs. O teste `tests/test_comparison_rpc.py` repete o log de 3000 pelo XML-RPC e confere que a última entrada volta a ser a tupla `(5, 2999)`. `tests/test_groupmngr.py` cobre o registro idempotente, a troca de porta e o `unregister`.

## Confiabilidade e FIFO

*Beatriz Gerogiannis.*

O multicast original entregava o datagrama uma vez e seguia. Perder um `READY` prendia quem esperava o handshake; perder o fim da rodada impedia gravar o log; perder um dado deixava um buraco sem aviso. A versão atual confirma cada `READY`, `DATA` e `END` e retransmite por timeout. Quem envia é que sabe o que falta: um NACK só apareceria quando chegasse uma mensagem posterior, e a última mensagem, ou o próprio `END`, não abre lacuna.

O receptor manda ACK também para a cópia repetida, porque o ACK anterior pode ter se perdido, e conta a duplicata à parte. Lacuna vira linha `Gap: from process ...`; quando a atrasada chega, `Late message ... fills a gap`. O resumo da rodada imprime mensagens entregues, retransmissões, duplicatas descartadas, lacunas e quantas ficaram retidas na fila.

A fila de espera é o FIFO. Chegar a sequência 7 com a 5 ainda faltando não grava o 7. Ele espera, e a entrega daquele remetente sai 5, 6, 7. Entre remetentes a intercalação de cada peer continua a sua. Por isso a métrica antiga de ordem total segue alta debaixo de perda: ela compara a posição global dos logs, e o FIFO não promete essa posição.

Antes da fila, o mesmo cenário (`delay 20ms 10ms loss 10%`, 6 peers, duas rodadas de 20) terminou com 6 a 11 violações de FIFO por log e `check_logs` em FAILED ([fifo-antes.txt](docs/evidencias/fifo-antes.txt)). Com a fila, os seis logs ficaram com 0 violações e 120 entradas ([fifo-depois.txt](docs/evidencias/fifo-depois.txt)), enquanto o comparisonServer ainda imprimia 120 e 119 rodadas fora de ordem na métrica antiga. Houve mensagens retidas (`held back`) em toda rodada.

No Docker, com o netem só na porta 6789, perda de 10% deu de 16 a 38 retransmissões por peer por rodada, e perda de 30% deu de 75 a 139, nas três execuções de cada uma, sempre com `check_logs` OK e sem linha `ERROR`.

## Simulação e verificação

*Geovanne.*

Há dois jeitos de estragar o UDP de propósito. No contêiner, `NETEM` usa `tc netem` e o filtro da seção de detalhes, e o peer imprime `tc qdisc show` e `tc filter show` ao subir. No processo, [netsim.py](netsim.py) decide em cada envio: `LOSS_RATE` é a chance de descartar em silêncio, `DELAY_MS` é a espera fixa e `JITTER_MS` soma um aleatório uniforme para os dois lados, com o resultado preso em zero ou mais. Os três em zero deixam o peer igual a um `sendto`. `tests/test_netsim.py` cobre o passthrough, o descarte e o atraso sem rede e sem Docker. Os dois mecanismos no mesmo experimento somam os efeitos; para um cenário só, use um deles.

A verificação da rodada está no comparisonServer e também no arquivo de log. `check_fifo_and_completeness` (métrica 1) olha cada remetente: exatamente `0 .. nMsgs-1`, sem duplicata, em ordem estritamente crescente. `count_unordered_rounds` (métrica 2) é a conta original, conservada para mostrar que FIFO e ordem total divergem. `tests/check_logs.py` lê `logs/logfile*.log` e exige `N * nMsgs` entradas, todos os pares `(remetente, seq)`, sem duplicata e com a subsequência de cada remetente igual a `0, 1, ..., nMsgs-1`. `--since EPOCH` recusa um log mais velho que o início do experimento, para um arquivo antigo não passar como OK.

O sistema já com RPC, confiabilidade e FIFO foi rodado de novo com `NETEM` só no UDP 6789, seis peers e `AUTO_ROUNDS=20,20`, no commit `13e0bc0`:

- [docs/evidencias/rpc-fifo-loss10.txt](docs/evidencias/rpc-fifo-loss10.txt) — `loss 10%`
- [docs/evidencias/rpc-fifo-delay20-loss10.txt](docs/evidencias/rpc-fifo-delay20-loss10.txt) — `delay 20ms 10ms loss 10%`

Nos dois o comparisonServer saiu com código 0, cada log tinha 120 mensagens, a métrica 1 passou nos seis peers nas duas rodadas e `check_logs.py` terminou OK. Apareceram retransmissão, duplicata descartada e mensagem retida na fila. A métrica 2 ficou entre 117 e 120. Não houve linha `ERROR` nem `WARNING`. O teste em nuvem não foi feito.
