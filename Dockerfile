# Use the official Golang image as a build stage
FROM nexus-proxy.phoenix.local:32239/golang:1.22.8 AS builder


# Set the working directory inside the container
WORKDIR /app


# Copy the Go modules manifest and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the Go application
RUN go build -o myapp ./cmd/main.go && ls -lah myapp

# Set environment variables (these can also be set at runtime)
ENV MONGO_URI="mongodb://localhost:27017"
ENV DB_NAME="mydatabase"
ENV COLLECTION="mycollection"
ENV INDEX_NAME="Myindex"
ENV INDEX_NAME_EXTERNAL="externalindex"
ENV ELASTIC_ADDR="https://localhost:9200"
ENV ELASTIC_USERNAME="elastic"
ENV ELASTIC_PASSWORD="123"
ENV TOPIC_NAME="message-log"
ENV KAFKA_ADDR="localhost:9092"
ENV KAFKA_USERNAME="username"
ENV KAFKA_PASSWORD="password"




RUN chmod +x myapp

RUN ls -lah ./myapp


CMD ["./myapp"]
