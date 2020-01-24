@Library('ratcheting') _

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
        rtMaven.deployer.artifactDeploymentPatterns.addExclude("*celesta-test*")
        rtMaven.deployer.deployArtifacts = (env.BRANCH_NAME == 'dev')
        buildInfo = Artifactory.newBuildInfo()
        buildInfo.env.capture = true

        def downloadSpec = """
                 {"files": [
                    {
                      "pattern": "warn/celesta/*/warnings.yml",
                      "build": "celesta :: dev/LATEST",
                      "target": "previous.yml",
                      "flat": "true"
                    }
                    ]
                }"""
        server.download spec: downloadSpec
        oldWarnings = readYaml file: 'previous.yml'
    }

    stage ('Spellcheck'){
        result = sh (returnStdout: true,
           script: 
              """for f in \$(find celesta-documentation/src/main/asciidoc/en -name '*.adoc'); do cat \$f | sed "s/-/ /g" | aspell --master=en --personal=./dict-en list; done | sort | uniq""")
              .trim()
        if (result) {
           echo "The following words are probaly misspelled:"
           echo result
           error "Please correct the spelling or add the words above to the dict-en."
        }
        
        result = sh (returnStdout: true,
           script: 
              """for f in \$(find celesta-documentation/src/main/asciidoc/ru -name '*.adoc'); do cat \$f | sed "s/-/ /g" | aspell --master=ru --personal=./dict-ru list; done | sort | uniq""")
              .trim()
        if (result) {
           echo "The following words are probaly misspelled:"
           echo result
           error "Please correct the spelling or add the words above to the dict-ru."
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
            rtMaven.run pom: 'pom.xml', goals: 'clean install -P dev', buildInfo: buildInfo
        }
    } finally {
        junit '**/surefire-reports/**/*.xml'
        step( [ $class: 'JacocoPublisher', execPattern: '**/target/jacoco.exec' ] )
        checkstyle pattern: '**/target/checkstyle-result.xml' //, canComputeNew: true, useDeltaValues: true, shouldDetectModules: true
        findbugs pattern: '**/target/spotbugsXml.xml'
    }

    stage ('Ratcheting') {
        def modules = ['celesta-sql',
                       'celesta-core',
                       'celesta-maven-plugin',
                       'celesta-system-services',
                       'dbschemasync',
                       'celesta-unit']
        def warningsMap = countWarnings modules
        writeYaml file: 'target/warnings.yml', data: warningsMap
        compareWarningMaps oldWarnings, warningsMap
    }

    if (env.BRANCH_NAME == 'dev') {
        stage ('Publish build info') {
            def uploadSpec = """
            {
             "files": [
                {
                  "pattern": "target/warnings.yml",
                  "target": "warn/celesta/${currentBuild.number}/warnings.yml"
                }
                ]
            }"""

            def buildInfo2 = server.upload spec: uploadSpec
            buildInfo.append(buildInfo2)
            server.publishBuildInfo buildInfo
        }
    }
}
