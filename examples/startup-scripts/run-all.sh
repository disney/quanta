for node in `cat node`
do
  ssh ec2-user@${node} "$1"
done
