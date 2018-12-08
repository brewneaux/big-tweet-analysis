#1/bin/bash

BUCKET=''
today=$(date +%Y%m%d)

aws s3 cp --no-progress /home/ec2-user/data/reply_full_details_premium "s3://$BUCKET/replies_$today"
aws s3 cp --no-progress /home/ec2-user/data/start_id "s3://$BUCKET/start_id"