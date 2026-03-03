import fs from "fs";
import path from "path";


const inputFilePath = path.join('response.json');
const outputFilePath = path.join('event_ids.txt');

try {
  const rawData = fs.readFileSync(inputFilePath, 'utf8');
    
  const data = JSON.parse(rawData);
  const ids = data["data"].map(({ id }) => id);
  const jsonOutput = JSON.stringify(ids, null, 2);
  
  fs.writeFileSync(outputFilePath, jsonOutput, 'utf8');
  
  console.log(`Successfully extracted IDs and saved to ${outputFilePath}`);

} catch (error) {
  console.error('An error occurred:', error);
}