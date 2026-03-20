FROM node:20-alpine

WORKDIR /app

# Install dependencies first for better caching
COPY package*.json ./
RUN npm install

# Copy source code
COPY . .

# Install tsx globally for convenience
RUN npm install -g tsx

# Expose port
EXPOSE 3000

# Start script
CMD ["tsx", "src/index.ts"]
