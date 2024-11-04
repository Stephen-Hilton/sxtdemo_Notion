cd /home/stephen/sxtdemo_Notion
git checkout main
git pull 
python3 -m venv venv
. ./venv/bin/activate
pip3 install pysteve --upgrade
pip3 install spaceandtime --upgrade

python3 ./src/sync_sxtlabs_telem.py

deactivate
