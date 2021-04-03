import org.apache.nifi.controller.ControllerService
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.components.state.Scope
import groovy.json.JsonOutput
import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import java.util.List
import java.util.Map
import java.util.HashMap
import org.apache.nifi.processor.io.StreamCallback;

def slurper = new JsonSlurper()

try {
	
	def bulletinBoardObj = slurper.parseText(new URL(BulletinUrl.value).getText("UTF-8")).bulletinBoard
	//log.info(bulletinBoardObj.toString())
	
	if (bulletinBoardObj.bulletins) {
		
		bulletinBoardObj.bulletins.each {  b ->
			
			// create flow file for each bulletin message
			FlowFile ff = session.create();
			ff = session.write(ff, {inputStream, outputStream ->
			   outputStream.write(b.bulletin.message.getBytes(StandardCharsets.UTF_8))
			} as StreamCallback)
			ff = session.putAttribute(ff, 'category', b.bulletin.category)
			ff = session.putAttribute(ff, 'sourceId', b.bulletin.sourceId)
			ff = session.putAttribute(ff, 'sourceName', b.bulletin.sourceName)
			ff = session.putAttribute(ff, 'level', b.bulletin.level)
			ff = session.putAttribute(ff, 'timestamp', b.bulletin.timestamp)
			session.transfer(ff, REL_SUCCESS)
			
		}
		
	}
	

} catch(e) {
	log.error('BulletinBoardCapture.groovy ', e)
}




