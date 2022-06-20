git clone https://github.com/wurstmeister/kafka-docker.git
cp docker-compose.yml ./kafka-docker
cd kafka-docker
docker-compose up -d zookeeper
docker-compose scale kafka=3

ipconfig
Адаптер Ethernet VirtualBox Host-Only Network: IPv4-адрес. . . . . . . . . . . . : 192.168.56.1

docker ps -a

python3 accept.py
python3 send.py