pipeline {

    agent {
        docker {
            image 'python:3.7'
            args '-v /var/run/docker.sock:/var/run/docker.sock'
        }
    }
    
    stages {
        stage('Test') {
            steps {
                sh 'pip install -r requirements.txt'
                sh 'py.test --junitxml test-report.xml --cov-report xml:coverage-report.xml --cov=logsight tests/'
            }
            post {
                always {
                    junit 'test-report.xml'
                    archiveArtifacts '*-report.xml'
                }
            }
        }
        stage('SonarQube') {
            steps {
                script {
                    def scannerHome = tool 'sonar-scanner-linux'
                    withSonarQubeEnv('logsight-sonarqube') {
                        sh """
                            ${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=logsight -Dsonar.branch.name=$BRANCH_NAME \
                                -Dsonar.sources=logsight -Dsonar.tests=tests/. \
                                -Dsonar.inclusions="**/*.py" \
                                -Dsonar.python.coverage.reportPaths=coverage-report.xml \
                                -Dsonar.test.reportPath=test-report.xml
                        """
                    }
                }
            }
        }
    }
}