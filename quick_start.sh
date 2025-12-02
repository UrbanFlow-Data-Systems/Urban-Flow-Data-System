#!/bin/bash

# Smart City Traffic Dashboard - Quick Start Script
# This script automates the setup and startup of all components

set -e

echo "======================================================================"
echo "     SMART CITY TRAFFIC DASHBOARD - QUICK START                      "
echo "======================================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    print_status "Docker is installed"
}

# Check if Docker Compose is installed
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    print_status "Docker Compose is installed"
}

# Check if Python is installed
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    print_status "Python 3 is installed"
}

# Check if Node.js is installed
check_node() {
    if ! command -v node &> /dev/null; then
        print_error "Node.js is not installed. Please install Node.js first."
        exit 1
    fi
    print_status "Node.js is installed"
}

# Create necessary directories
create_directories() {
    echo ""
    echo "Creating project directories..."
    
    mkdir -p spark_jobs/checkpoints
    mkdir -p spark_logs
    mkdir -p airflow/dags
    mkdir -p airflow/logs
    mkdir -p airflow/plugins
    mkdir -p reports
    mkdir -p backend
    
    print_status "Directories created"
}

# Start Docker services
start_docker_services() {
    echo ""
    echo "Starting Docker services..."
    
    docker-compose up -d
    
    print_status "Docker services started"
    print_warning "Waiting 30 seconds for services to initialize..."
    sleep 30
}

# Install Python dependencies for backend
setup_backend() {
    echo ""
    echo "Setting up Flask API backend..."
    
    cd backend
    
    if [ ! -f "requirements.txt" ]; then
        cat > requirements.txt << EOL
flask==3.0.0
flask-cors==4.0.0
psycopg2-binary==2.9.9
EOL
    fi
    
    pip3 install -r requirements.txt
    
    cd ..
    print_status "Backend dependencies installed"
}

# Setup React frontend
setup_frontend() {
    echo ""
    echo "Setting up React dashboard..."
    
    if [ ! -d "frontend" ]; then
        print_warning "Creating React app (this may take a few minutes)..."
        npx create-react-app frontend
    fi
    
    cd frontend
    
    print_warning "Installing React dependencies..."
    npm install recharts lucide-react
    npm install -D tailwindcss postcss autoprefixer
    
    # Initialize Tailwind if not already done
    if [ ! -f "tailwind.config.js" ]; then
        npx tailwindcss init -p
    fi
    
    cd ..
    print_status "Frontend setup complete"
}

# Function to start Kafka producer
start_producer() {
    echo ""
    echo "Starting Kafka producer..."
    python3 kafka_producer.py > logs/producer.log 2>&1 &
    PRODUCER_PID=$!
    echo $PRODUCER_PID > .producer.pid
    print_status "Kafka producer started (PID: $PRODUCER_PID)"
}

# Function to start Spark streaming
start_spark() {
    echo ""
    echo "Starting Spark streaming job..."
    docker exec -d spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        --conf spark.cores.max=2 \
        /opt/spark-jobs/spark_streaming.py
    print_status "Spark streaming job submitted"
}

# Function to start Flask API
start_api() {
    echo ""
    echo "Starting Flask API..."
    cd backend
    python3 api.py > ../logs/api.log 2>&1 &
    API_PID=$!
    echo $API_PID > ../.api.pid
    cd ..
    print_status "Flask API started (PID: $API_PID)"
}

# Function to start React dashboard
start_dashboard() {
    echo ""
    echo "Starting React dashboard..."
    cd frontend
    npm start > ../logs/dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    echo $DASHBOARD_PID > ../.dashboard.pid
    cd ..
    print_status "React dashboard started (PID: $DASHBOARD_PID)"
}

# Main setup function
main() {
    echo "Checking prerequisites..."
    check_docker
    check_docker_compose
    check_python
    check_node
    
    create_directories
    
    # Create logs directory
    mkdir -p logs
    
    # Ask user what to do
    echo ""
    echo "What would you like to do?"
    echo "1) Full setup (first time)"
    echo "2) Start all services"
    echo "3) Stop all services"
    echo "4) View service status"
    read -p "Enter choice [1-4]: " choice
    
    case $choice in
        1)
            echo ""
            echo "======================================================================"
            echo "                    FULL SETUP MODE                                   "
            echo "======================================================================"
            
            setup_backend
            setup_frontend
            start_docker_services
            
            print_warning "Waiting for Docker services to be fully ready..."
            sleep 30
            
            start_producer
            sleep 5
            start_spark
            sleep 5
            start_api
            sleep 5
            start_dashboard
            
            echo ""
            echo "======================================================================"
            print_status "ALL SERVICES STARTED SUCCESSFULLY!"
            echo "======================================================================"
            echo ""
            echo "Access points:"
            echo "  🌐 Dashboard:       http://localhost:3000"
            echo "  🔧 Flask API:       http://localhost:5000"
            echo "  ⚡ Spark Master UI: http://localhost:8080"
            echo "  📋 Airflow UI:      http://localhost:8081 (admin/admin123)"
            echo ""
            echo "Log files:"
            echo "  📄 Producer:  logs/producer.log"
            echo "  📄 API:       logs/api.log"
            echo "  📄 Dashboard: logs/dashboard.log"
            echo ""
            print_warning "Press Ctrl+C to stop all services"
            ;;
            
        2)
            echo ""
            echo "======================================================================"
            echo "                    STARTING SERVICES                                 "
            echo "======================================================================"
            
            start_docker_services
            sleep 10
            start_producer
            sleep 5
            start_spark
            sleep 5
            start_api
            sleep 5
            start_dashboard
            
            echo ""
            print_status "All services started!"
            echo ""
            echo "Access Dashboard at: http://localhost:3000"
            ;;
            
        3)
            echo ""
            echo "Stopping all services..."
            
            # Stop Python services
            if [ -f .producer.pid ]; then
                kill $(cat .producer.pid) 2>/dev/null || true
                rm .producer.pid
                print_status "Producer stopped"
            fi
            
            if [ -f .api.pid ]; then
                kill $(cat .api.pid) 2>/dev/null || true
                rm .api.pid
                print_status "API stopped"
            fi
            
            if [ -f .dashboard.pid ]; then
                kill $(cat .dashboard.pid) 2>/dev/null || true
                rm .dashboard.pid
                print_status "Dashboard stopped"
            fi
            
            # Stop Docker services
            docker-compose down
            print_status "Docker services stopped"
            
            echo ""
            print_status "All services stopped!"
            ;;
            
        4)
            echo ""
            echo "======================================================================"
            echo "                    SERVICE STATUS                                    "
            echo "======================================================================"
            echo ""
            
            echo "Docker Services:"
            docker-compose ps
            
            echo ""
            echo "Python Services:"
            if [ -f .producer.pid ]; then
                if ps -p $(cat .producer.pid) > /dev/null; then
                    print_status "Producer running (PID: $(cat .producer.pid))"
                else
                    print_error "Producer not running"
                fi
            else
                print_error "Producer not started"
            fi
            
            if [ -f .api.pid ]; then
                if ps -p $(cat .api.pid) > /dev/null; then
                    print_status "API running (PID: $(cat .api.pid))"
                else
                    print_error "API not running"
                fi
            else
                print_error "API not started"
            fi
            
            if [ -f .dashboard.pid ]; then
                if ps -p $(cat .dashboard.pid) > /dev/null; then
                    print_status "Dashboard running (PID: $(cat .dashboard.pid))"
                else
                    print_error "Dashboard not running"
                fi
            else
                print_error "Dashboard not started"
            fi
            ;;
            
        *)
            print_error "Invalid choice"
            exit 1
            ;;
    esac
}

# Handle Ctrl+C
trap 'echo ""; echo "Stopping all services..."; kill 0; docker-compose down; exit' INT

# Run main function
main