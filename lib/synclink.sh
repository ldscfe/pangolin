# synclink.sh pcp
if (( $# >= 1 )); then
    UP_PATH=$1
else
    echo 'less PATH.'
    exit 1
fi

cd ../${UP_PATH}
ln -s ../lib/etl_unit.py .
ln -s ../lib/pet_usql_ch.py .
ln -s ../lib/pet_usql_gp.py .
ln -s ../lib/pet_usql_mysql.py .
ln -s ../lib/pet_usql_oracle.py .
ln -s ../lib/pet_usql.py .

echo 'Success.'
