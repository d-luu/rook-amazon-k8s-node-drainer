# Create build environment

The following instructions have been tested on Amazon Linux
For OSX, install homebrew and make sure docker is running, then skip to step 12.


1) Update package cache
```bash
sudo yum update -y
```

2) Install Docker
```bash
sudo yum install docker -y
```

3) Install Git
```bash
sudo yum install git -y
```

4) Install development tools
```bash
sudo yum groupinstall 'Development Tools' -y
```

5) Start Docker
```bash
sudo service docker start
```

6) Add ec2-user to docker group to execute without `sudo`
```bash
sudo usermod -a -G docker ec2-user
```

7) Log out and log back in again for changes to take affect

8) Verify ec2-user can run docker commands without `sudo`
```bash
docker ps
```

9a) Install Homebrew for Linux
```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install.sh)"
```

10) Add brew to bash profile
```bash
test -d ~/.linuxbrew && eval $(~/.linuxbrew/bin/brew shellenv)
test -d /home/linuxbrew/.linuxbrew && eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)
test -r ~/.bash_profile && echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.bash_profile
echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.profile
```

11) Verify brew is installed
```bash
brew --version
```

12) Install AWS SAM CLI
```bash
brew tap aws/tap
brew install aws-sam-cli
```

13) Verify AWS SAM CLI is installed
```bash
sam --version
```


# Build rook-amazon-k8s-node-drainer

1) Clone repo
```bash
git clone https://github.com/d-luu/rook-amazon-k8s-node-drainer.git
```

1) Setup credentials (use `read -s` to hide from history)
```bash
export AWS_REGION="us-west-2"
export AWS_DEFAULT_REGION="us-west-2"  # used for boto3
export AWS_ACCESS_KEY_ID="${YOUR_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${YOUR_SECRET_ACCESS_KEY}"
```

2) If needed, create S3 bucket
```bash
aws s3 mb s3://${BUCKET_NAME}
```

3) Change to rook-amazon-k8s-node-drainer folder
```bash
cd rook-amazon-k8s-node-drainer
```

4) Build
```bash
sam build --use-container --skip-pull-image
```

5) Push artifact to S3 bucket and output template file
```bash
sam package \
    --output-template-file packaged.yaml \
    --s3-bucket ${BUCKET_NAME}
```

6) Push AWS Lambda Cloudformation Stack template file to S3 as well
```bash
aws s3 cp packaged.yaml s3://${BUCKET_NAME}
```

7) Create AWS Lambda Cloudformation Stack from template file
```bash
sam deploy \
    --template-file packaged.yaml \
    --stack-name "${YOUR_CLUSTER_NAME}-${YOUR_AUTOSCALING_GROUP_NAME}-drainer" \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides AutoScalingGroup="${YOUR_AUTOSCALING_GROUP_NAME}" EksCluster="${YOUR_CLUSTER_NAME}"
```


# Settings for rook-amazon-k8s-node-drainer

## Environment variables

cluster_name: the name of the EKS cluster
 * optional
   * either this must be set, or `kube_config_bucket` and `kube_config_object` must be set
   
kube_config_bucket: the bucket where a kube config is stored in S3
 * for use against a non-EKS cluster
 * must be used with `kube_config_object`
 * optional
   * either this must be set or `cluster_name` must be set
   
kube_config_object: the object where a kube config is stored in S3
 * for use against a non-EKS cluster
 * must be used with `kube_config_bucket`
 * optional
   * either this must be set or `cluster_name` must be set
   
rook_ceph_volumes_namespace: the rook ceph namespace where the rook volumes reside (and also where the operator resides)
 * for use in Rook deployments
 * defaults to `rook-ceph`
 * optional

detach_rook_volumes: indicates whether to detach rook ceph volumes from drained/deleted nodes
 * for use in Rook deployments
 * `rook_ceph_operator_namespace` must be non-empty
 * defaults to 'true'
 * optional
 
rook_ceph_crashcollectors_namespace: the rook ceph namespace where the crash collectors reside
 * for use in Rook deployments
 * default to `rook-ceph`
 * optional

delete_rook_ceph_crashcollector: indicates whether to delete the rook ceph crash collector from drained/deleted nodes
 * for use in Rook deployments
 * `rook_ceph_crashcollectors_namespace` must be non-empty
 * defaults to 'true'
 * optional

delete_node: indicates whether to delete the node after draining
 * defaults to 'true'
 * optional

pod_eviction_timeout: the time, in seconds, to wait for pod eviction to complete before continuing
 * defaults to 120

pod_delete_grace_period: the grace period, in seconds, to set for the leftover failed to evict pods for deletion
 * runs after pod eviction
 * optional
