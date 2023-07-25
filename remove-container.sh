ssh-keygen -R '[localhost]:58222'
pkill -f "ssh -i $(pwd)/ssh_tunnel/tunnel_rsa tunnel@localhost -p 58222"
docker stop go_mux_postgresql_gorm -t 1
docker rm --force go_mux_postgresql_gorm
docker rmi --force go_mux_postgresql_gorm
docker stop go_mux_postgresql_gorm_ssh -t 1
docker rm --force go_mux_postgresql_gorm_ssh
docker rmi --force go_mux_postgresql_gorm_ssh
docker rmi --force public.ecr.aws/o2c0x5x8/community-images-backup:lscr.io-linuxserver-openssh-server
docker rmi --force public.ecr.aws/o2c0x5x8/application-base:go-mux-postgresql-gorm
docker system prune
docker image prune -a
docker volume prune