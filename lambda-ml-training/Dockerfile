FROM public.ecr.aws/lambda/python:3.9

# Install system dependencies
RUN yum update -y && \
    yum install -y gcc gcc-c++ make && \
    yum clean all

# Copy requirements file
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy training code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY training/ ${LAMBDA_TASK_ROOT}/training/

# Set the CMD to your handler
CMD ["lambda_function.lambda_handler"]
