pipeline {
    agent any

    environment {
        // see https://github.com/git-lfs/git-lfs/issues/4248
        GIT_LFS_SKIP_SMUDGE = 1
    }

    stages {
        stage('Test') {
            agent {
                docker {
                    image 'python:3.7'
                }
            }
            steps {
                sh 'pip install -r requirements.txt'
                sh 'PYTHONPATH=$PWD/logsight py.test --junitxml test-report.xml --cov-report xml:coverage-report.xml --cov=logsight tests/'
                stash name: 'test-reports', includes: '*.xml' 
            }
            post {
                always {
                    junit 'test-report.xml'
                    archiveArtifacts '*-report.xml'
                }
            }
        }
        stage('Linting & SonarQube') {
            parallel {
                stage('SonarQube') {
                    agent {
                        docker {
                            image 'sonarsource/sonar-scanner-cli'
                        }
                    }
                    steps {
                        script {
                            unstash "test-reports"
                            withSonarQubeEnv('logsight-sonarqube') {
                                sh """ 
                                    sonar-scanner -Dsonar.projectKey=logsight -Dsonar.branch.name=$BRANCH_NAME \
                                        -Dsonar.sources=logsight -Dsonar.tests=tests/. \
                                        -Dsonar.inclusions="**/*.py" \
                                        -Dsonar.python.coverage.reportPaths=coverage-report.xml \
                                        -Dsonar.test.reportPath=test-report.xml
                                """
                            }
                        }
                    }
                }
                stage ("Lint Dockerfile") {
                    agent {
                        docker {
                            image 'hadolint/hadolint:latest-debian'
                        }
                    }
                    steps {
                        sh 'hadolint --format checkstyle Dockerfile | tee -a hadolint.xml'
                    }
                    post {
                        always {
                            archiveArtifacts 'hadolint.xml'
                        }
                    }
                }
            }
        }
        stage ("Build Docker Images") {
            steps {
               sh "echo test"
            }
        }
    }
}