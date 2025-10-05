# Use Node.js 20
FROM node:20

WORKDIR /app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy script and CSV
COPY . .

# Run the ingestion script
CMD ["node", "ingest_trades.js"]
