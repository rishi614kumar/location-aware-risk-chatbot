# GeoRisk AI
An Agentic Geospatial AI Chatbot for Autonomous Multi-Domain Urban Risk Analysis.

## Setup Instructions

### Prerequisites
1. **Python**: Ensure Python 3.8+ is installed on your system.
2. **Dependencies**: Install the required Python packages using `pip`.
3. **Chainlit**: Install Chainlit globally using pip if not already installed:
   ```bash
   pip install chainlit
   ```

### Environment Configuration
1. Create a `.env` file in the root directory of the project (if it doesn't already exist).
2. Add the following environment variables to the `.env` file:

   ```env
   GEOCLIENT_API_KEY=<your_geoclient_api_key>
   MAPPLUTO_GDB_PATH=<path_to_mappluto_gdb>
   LION_GDB_PATH=<path_to_lion_gdb>
   NTA_PATH=https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson
   GEMINI_API_KEY=<your_gemini_api_key>
   CRIME_PATH=<local path to downloaded crime data file>
   CHAINLIT_AUTH_SECRET=<a secure token or random string>
   CHAINLIT_ADMIN_USER=<admin_username>
   CHAINLIT_ADMIN_PASSWORD=<admin_password>
   CHAINLIT_DB_URL=sqlite+aiosqlite:///./chainlit_history.db
   ```

   Download the crime data XLS from: [NYPD Historical Crime Data – Seven Major Felony Offenses by Precinct (2000–2024)](https://www.nyc.gov/assets/nypd/downloads/excel/analysis_and_planning/historical-crime-data/seven-major-felony-offenses-by-precinct-2000-2024.xls)

   Replace the placeholders (e.g., `<your_geoclient_api_key>`) with the actual values for your environment.

   You can generate a secure value for `CHAINLIT_AUTH_SECRET` with:

   ```bash
   chainlit create-secret
   ```

   Copy the generated secret into your `.env` file.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/rishi614kumar/location-aware-risk-chatbot.git
   ```
2. Navigate to the project directory:
   ```bash
   cd location-aware-risk-chatbot
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Application
Run the application using Chainlit:
```bash
chainlit run app.py -w
```

This will start the chatbot application, and you can interact with it through the Chainlit interface.