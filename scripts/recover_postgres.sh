sudo rm /root/autodl-tmp/postgresql/data/main/postgresql.auto.conf
sleep 2
su - postgres -c '/usr/lib/postgresql/14/bin/pg_ctl restart -D /root/autodl-tmp/postgresql/data/main/ -o "-c config_file=/etc/postgresql/14/main/postgresql.conf"'