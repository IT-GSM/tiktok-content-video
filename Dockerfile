# Use an official Python runtime as a parent image
FROM python:3.9-slim
#FROM apify/actor-python-playwright:3.11.6
# FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

# # Set the working directory in the container
# WORKDIR /usr/src/app

# # Copy the current directory contents into the container at /app
# COPY . .

# # python:3.9-alpine mcr.microsoft.com/playwright:focal
# # Install necessary dependencies for Chrome
# RUN apt-get update && apt-get install -y python3-pip \
#     wget \
#     gnupg \
#     ca-certificates \
#     fonts-liberation \
#     libappindicator3-1 \
#     libasound2 \
#     libatk-bridge2.0-0 \
#     libatk1.0-0 \
#     libcups2 \
#     libdbus-1-3 \
#     libdrm2 \
#     libgbm1 \
#     libgdk-pixbuf2.0-0 \
#     libgtk-3-0 \
#     libnspr4 \
#     libnss3 \
#     libx11-6 \
#     libxcomposite1 \
#     libxdamage1 \
#     libxext6 \
#     libxfixes3 \
#     libxrandr2 \
#     xdg-utils \
#     --no-install-recommends \
#     && rm -rf /var/lib/apt/lists/*

# # Set up Chrome repository and install Chrome
# RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
#     && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
#     && apt-get update && apt-get install -y google-chrome-stable \
#     && rm -rf /var/lib/apt/lists/*

# # Create and activate a virtual environment
# # RUN apt install python3.11-venv

# # Install pip inside the virtual environment and update it
# # RUN ./venv/bin/pip install --upgrade pip

# # Install Python dependencies in the virtual environment
# # RUN ./venv/bin/pip3 install -r requirements.txt

# # Install Python and venv package
# RUN apt update && apt install -y python3.11 python3.11-venv

# # Create and activate virtual environment
# RUN python3.11 -m venv venv

# # Upgrade pip, setuptools, and wheel
# RUN /venv/bin/pip install --upgrade pip setuptools wheel

# # Install dependencies
# RUN /venv/bin/pip install --no-cache-dir -r requirements.txt

# #RUN python -m playwright install
# # Install any needed dependencies specified in requirements.txt
# #RUN pip3 install -r requirements.txt

# # Install Playwright inside the virtual environment
# RUN ./venv/bin/pip3 install playwright && ./venv/bin/playwright install

# # Make the virtual environment the default Python environment
# ENV PATH="/usr/src/app/venv/bin:$PATH"

# RUN playwright install
# Run source.py script
# RUN python3 app.py
# COPY app.py /app

# Use the official Playwright image as the base image
# FROM mcr.microsoft.com/playwright:focal

# # Install Python 3.9 and pip
# RUN apt-get update && apt-get install -y python3.9 python3.9-distutils python3-pip

# # Set the working directory
# WORKDIR /app

# # Copy the current directory contents into the container at /app
# COPY . .

# # Install the Python dependencies
# RUN pip3 install -r requirements.txt

# # Install Playwright dependencies
# RUN python3 -m playwright install

# Command to run your application
# CMD ["python3", "video.py"]

# Use the official Playwright image as the base image
# FROM mcr.microsoft.com/playwright:focal

# # Install Python 3.9 and pip
# # RUN apt-get update && apt-get install -y python3.9 python3.9-venv python3.9-distutils python3-pip
# RUN apt-get update && apt-get install -y python3.9 python3.9-venv python3.9-distutils python3-pip xvfb
# # Set the working directory
# WORKDIR /app

# # Copy the current directory contents into the container at /app
# COPY . .

# # Create a virtual environment
# RUN python3.9 -m venv venv

# # Activate the virtual environment and install the Python dependencies
# RUN . venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# # Install Playwright dependencies inside the virtual environment
# RUN . venv/bin/activate && python3 -m playwright install

# # Make the virtual environment the default Python environment
# ENV PATH="/app/venv/bin:$PATH"

# # Command to run your application
# # CMD ["python3.9", "video.py"]
# CMD ["xvfb-run", "-a", "python3.9", "video.py"]


# Use the official Python image as the base image
FROM python:3.11.6-slim

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    xvfb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Create a virtual environment
RUN python -m venv venv

# Activate the virtual environment and install the Python dependencies
RUN . venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright dependencies inside the virtual environment
RUN . venv/bin/activate && python -m playwright install

# Make the virtual environment the default Python environment
ENV PATH="/app/venv/bin:$PATH"

# Command to run your application with xvfb-run
CMD ["xvfb-run", "-a", "python", "hashtag.py"]

