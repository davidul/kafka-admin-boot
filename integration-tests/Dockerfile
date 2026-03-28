FROM python:3.11.2-alpine3.17

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /integration_tests

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r /integration_tests/requirements.txt

# Run pytest when the container launches
CMD ["pytest", "/integration_tests/get_topics.py"]