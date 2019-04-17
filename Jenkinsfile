node {
    def server = Artifactory.server 'ART'
    def rtMaven = Artifactory.newMavenBuild()
    def buildInfo
    def oldWarnings

    stage ('Clone') {
        checkout scm
    }

    stage ('Artifactory configuration') {
        rtMaven.tool = 'M3'
        rtMaven.deployer releaseRepo: 'libs-release-local', snapshotRepo: 'libs-snapshot-local', server: server
        rtMaven.resolver releaseRepo: 'libs-release', snapshotRepo: 'libs-snapshot', server: server
        rtMaven.deployer.artifactDeploymentPatterns.addExclude("*celesta-test*").addExclude("*dbschemasync*")
        rtMaven.deployer.deployArtifacts = (env.BRANCH_NAME == 'dev6')
        buildInfo = Artifactory.newBuildInfo()
        buildInfo.env.capture = true
    }

    stage ('Spellcheck'){
        result = sh (returnStdout: true,
           script: """for f in \$(find celesta-documentation -name '*.adoc'); do cat \$f | sed "s/-/ /g" | aspell --master=ru --personal=./dict list; done | sort | uniq""")
              .trim()
        if (result) {
           echo "The following words are probaly misspelled:"
           echo result
           error "Please correct the spelling or add the words above to the local dictionary."
        }
    }

    stage ('Docker cleanup') {
        sh '''docker ps -a -q &> /dev/null
if [ $? != 0 ]; then
   docker rm $(docker ps -a -q)
fi'''
    }

    try{
        stage ('Exec Maven') {
            rtMaven.run pom: 'pom.xml', goals: 'clean install', buildInfo: buildInfo
        }
    } finally {
        junit '**/surefire-reports/**/*.xml'
    }

    if (env.BRANCH_NAME == 'dev6') {
        stage ('Publish build info') {
            server.publishBuildInfo buildInfo
        }
    }
}
