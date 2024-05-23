# Navigate to the directory containing your Lambda function
cd ~/environment/semantic-search-invoke-pretrain-all-MiniLM-L6-v2-dev/hello_world


# Example for Python Lambda functions: Install dependencies into the current directory
pip install -r requirements.txt -t ./package/


#Add your Lambda function code to the package directory
cp app.py package/
cd package 

# Create a zip file named lambda-function.zip including all files and directories in the current directory
zip -r ../Invoke-sagemaker-pretrain.zip .
