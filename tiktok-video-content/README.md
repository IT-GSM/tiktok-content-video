# TikTok Video Content Project

This project is designed to interact with the TikTok API to fetch user profile data and videos. It utilizes asynchronous programming to efficiently handle API requests and store the retrieved data in a PostgreSQL database.

## Project Structure

```
tiktok-video-content
├── ind.py                # Main logic for interacting with the TikTok API
├── requirements.txt      # Python dependencies required for the project
├── Dockerfile            # Instructions to build a Docker image for the project
└── README.md             # Documentation for the project
```

## Requirements

Before running the project, ensure you have the following installed:

- Docker
- Python 3.x (for local development)

## Installation

1. Clone the repository:

   ```
   git clone <repository-url>
   cd tiktok-video-content
   ```

2. Install the required Python packages (if running locally):

   ```
   pip install -r requirements.txt
   ```

## Docker Setup

To build and run the Docker container, follow these steps:

1. Build the Docker image:

   ```
   docker build -t tiktok-video-content .
   ```

2. Run the Docker container:

   ```
   docker run -e ms_token=<your_ms_token> tiktok-video-content
   ```

   Replace `<your_ms_token>` with your actual TikTok `ms_token`.

## Usage

The main functionality is encapsulated in the `ind.py` file, which defines the `UserInfo` class. You can modify the `sources` variable in the `if __name__ == "__main__":` block to specify which TikTok users to fetch data from.

## Contributing

Feel free to submit issues or pull requests if you have suggestions or improvements for the project.

## License

This project is licensed under the MIT License. See the LICENSE file for details.