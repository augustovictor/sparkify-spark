CLUSTER_NAME=sparkify-cluster
EC2_INSTANCE_PROFILE=EMR_EC2_DefaultRole
KEY_NAME=sparkify
AWS_PROFILE=sparkify.emr

aws-set-profile:
	$(shell export AWS_PROFILE=$(AWS_PROFILE))
	echo $($$AWS_PROFILE)
	# j-23FSC1ZP0J76G

cluster-up:
	aws --profile $(AWS_PROFILE) emr create-cluster --name $(CLUSTER_NAME) \
	--use-default-roles \
	--release-label emr-5.28.0 \
	--instance-count 2 \
	--applications Name=Spark \
	--ec2-attributes KeyName=$(KEY_NAME) \
	--instance-type m3.xlarge \
	--instance-count 3 #\
	# --auto-terminate

cluster-describe:
	aws --profile $(AWS_PROFILE) emr describe-cluster --cluster-id $(cluster-id)