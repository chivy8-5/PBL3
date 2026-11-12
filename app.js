const BROKER_URL = "wss://broker.emqx.io:8084/mqtt";
const clientId="web_client_"+Math.random().toString(16).substr(2,8);
let mqttClient=null;
let currentSubTopic=null;
let desiredSubTopic=null;
let sensorChart=null;
let lastSensorTime=null;
const MAX_CHART_POINTS=60;
const deviceState={power:false,auto:false,fanSpeed:0,sleep:false,timer:0};
function getElement(id){return document.getElementById(id)}
function setText(id,value){const el=getElement(id);if(el)el.textContent=value}
function toValidNumber(value){if(value===undefined||value===null||value===""||typeof value==="boolean")return null;const n=Number(value);return Number.isFinite(n)?n:null}
function formatNumber(value){if(value===null||value===undefined)return"--";return Number(value).toLocaleString("vi-VN",{maximumFractionDigits:2})}
function formatDateTime(date){return date.toLocaleString("vi-VN",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false})}
function updateMQTTStatus(text,state="waiting"){const box=getElement("mqttIndicator");if(!box)return;let color="#f0b94b";if(state==="connected")color="#59d39d";if(state==="error")color="#e65b63";box.innerHTML=`<span class="dot" style="background:${color};box-shadow:0 0 0 5px ${state==="connected"?"rgba(89,211,157,.13)":state==="error"?"rgba(230,91,99,.13)":"rgba(240,185,75,.13)"}"></span><span>${text}</span>`}
function connectToBroker(){const options={keepalive:60,clientId:clientId,protocolId:"MQTT",protocolVersion:4,clean:true,reconnectPeriod:2000,connectTimeout:30000};mqttClient=mqtt.connect(BROKER_URL,options);mqttClient.on("connect",()=>{console.log("Kết nối MQTT thành công:",clientId);updateMQTTStatus("Đã kết nối MQTT","connected");if(desiredSubTopic){mqttClient.subscribe(desiredSubTopic,{qos:0},err=>{if(err){console.error(err);updateMQTTStatus("Subscribe thất bại","error");return}currentSubTopic=desiredSubTopic;updateMQTTStatus("Đang nhận dữ liệu","connected")})}});mqttClient.on("error",err=>{console.error("MQTT lỗi:",err);updateMQTTStatus("Kết nối MQTT thất bại","error")});mqttClient.on("close",()=>{currentSubTopic=null;updateMQTTStatus("Mất kết nối MQTT","error")});mqttClient.on("reconnect",()=>{updateMQTTStatus("Đang kết nối lại MQTT...","waiting")});mqttClient.on("message",(topic,message)=>{const payload=message.toString();console.log("MQTT RX:",topic,payload);if(topic===currentSubTopic)handleSensorData(payload)})}
function subscribeTopic(){if(!mqttClient||!mqttClient.connected){alert("Chưa kết nối MQTT Broker!");return}const input=getElement("subTopic");if(!input)return;const topic=input.value.trim();if(!topic){alert("Vui lòng nhập Topic Subscribe!");return}const doSubscribe=()=>{mqttClient.subscribe(topic,{qos:0},err=>{if(err){console.error("Subscribe lỗi:",err);updateMQTTStatus("Subscribe thất bại","error");return}currentSubTopic=topic;desiredSubTopic=topic;updateMQTTStatus("Đang nhận: "+topic,"connected");console.log("Đã subscribe:",topic)})};if(currentSubTopic&&currentSubTopic!==topic){mqttClient.unsubscribe(currentSubTopic,err=>{if(err){console.error(err);return}currentSubTopic=null;desiredSubTopic=null;doSubscribe()})}else doSubscribe()}
function unsubscribeTopic(){if(!mqttClient||!mqttClient.connected){alert("Chưa kết nối MQTT Broker!");return}if(!currentSubTopic){alert("Không có topic để unsubscribe!");return}const oldTopic=currentSubTopic;mqttClient.unsubscribe(oldTopic,err=>{if(err){console.error("Unsubscribe lỗi:",err);return}currentSubTopic=null;desiredSubTopic=null;updateMQTTStatus("Đã unsubscribe: "+oldTopic,"waiting");console.log("Đã unsubscribe:",oldTopic)})}
function setBadge(id,text,className){const el=getElement(id);if(!el)return;el.textContent=text;el.style.background=className==="good"?"#20a35b":className==="warning"?"#e7a400":className==="danger"?"#e53e4d":"#94a3b8"}
function updateTemperatureStatus(value){if(value<25)setBadge("tempBadge","COLD","danger");else if(value<=26)setBadge("tempBadge","GOOD","good");else setBadge("tempBadge","HOT","danger")}
function updateHumidityStatus(value){if(value<30)setBadge("humBadge","THẤP","warning");else if(value<=60)setBadge("humBadge","GOOD","good");else setBadge("humBadge","CAO","warning")}
function updateDustStatus(value){const badge=getElement("pmBadge");const message=getElement("aqiMessage");const ring=getElement("airRing");if(value<=9){if(badge){badge.textContent="TỐT";badge.style.background="#def4ec";badge.style.color="#2a8e73"}if(message)message.textContent="Chất lượng không khí tốt";if(ring)ring.style.borderColor="#4cb69a"}else if(value<=35.4){if(badge){badge.textContent="TRUNG BÌNH";badge.style.background="#fff0c9";badge.style.color="#b77900"}if(message)message.textContent="Chất lượng không khí trung bình";if(ring)ring.style.borderColor="#e7a400"}else{if(badge){badge.textContent="CAO";badge.style.background="#ffe0e3";badge.style.color="#c93d49"}if(message)message.textContent="Nồng độ bụi PM2.5 cao";if(ring)ring.style.borderColor="#e53e4d"}}
function handleSensorData(payload){let data;try{data=JSON.parse(payload)}catch(error){console.log("Message không phải JSON:",payload);return}if(!data||typeof data!=="object"||Array.isArray(data))return;const temperature=toValidNumber(data.temperature);const humidity=toValidNumber(data.humidity);const dust=toValidNumber(data.dust??data.pm25??data.pm2_5);if(temperature!==null){setText("temperature",formatNumber(temperature));updateTemperatureStatus(temperature)}if(humidity!==null){setText("humidity",formatNumber(humidity));updateHumidityStatus(humidity)}if(dust!==null){setText("pm25Value",formatNumber(dust));setText("machineValue",formatNumber(dust));updateDustStatus(dust)}if(temperature===null&&humidity===null&&dust===null){handleDeviceFeedback(data);return}lastSensorTime=new Date();setText("lastUpdate",formatDateTime(lastSensorTime));addChartPoint(lastSensorTime,temperature,humidity,dust);handleDeviceFeedback(data)}
function initializeChart(){const canvas=getElement("sensorChart");if(!canvas||typeof Chart==="undefined")return;sensorChart=new Chart(canvas,{type:"line",data:{labels:[],datasets:[{label:"PM2.5",data:[],borderColor:"#29956c",backgroundColor:"rgba(41,149,108,.08)",borderWidth:3,pointRadius:2,tension:.3,spanGaps:true,yAxisID:"yDust"},{label:"Nhiệt độ",data:[],borderColor:"#e99151",backgroundColor:"rgba(233,145,81,.06)",borderWidth:2,borderDash:[8,6],pointRadius:2,tension:.3,spanGaps:true,yAxisID:"yEnvironment"},{label:"Độ ẩm",data:[],borderColor:"#4c83ce",backgroundColor:"rgba(76,131,206,.06)",borderWidth:2,borderDash:[8,6],pointRadius:2,tension:.3,spanGaps:true,yAxisID:"yEnvironment"}]},options:{responsive:true,maintainAspectRatio:false,animation:false,interaction:{mode:"index",intersect:false},plugins:{legend:{display:false},tooltip:{enabled:true}},scales:{x:{ticks:{maxTicksLimit:8,color:"#71857f"},grid:{color:"rgba(70,105,95,.08)"}},yEnvironment:{type:"linear",position:"left",ticks:{color:"#71857f"},grid:{color:"rgba(70,105,95,.08)"},title:{display:true,text:"Nhiệt độ / Độ ẩm"}},yDust:{type:"linear",position:"right",ticks:{color:"#71857f"},grid:{drawOnChartArea:false},title:{display:true,text:"PM2.5"}}}}})}
function addChartPoint(time,temperature,humidity,dust){if(!sensorChart)return;sensorChart.data.labels.push(time.toLocaleTimeString("vi-VN",{hour12:false}));sensorChart.data.datasets[0].data.push(dust);sensorChart.data.datasets[1].data.push(temperature);sensorChart.data.datasets[2].data.push(humidity);while(sensorChart.data.labels.length>MAX_CHART_POINTS){sensorChart.data.labels.shift();sensorChart.data.datasets.forEach(dataset=>dataset.data.shift())}sensorChart.update("none")}
function publishMQTT(data){if(!mqttClient||!mqttClient.connected){alert("Chưa kết nối MQTT Broker!");return false}const input=getElement("pubTopic");if(!input)return false;const topic=input.value.trim();if(!topic){alert("Vui lòng nhập Topic Publish!");return false}const payload=JSON.stringify(data);mqttClient.publish(topic,payload,{qos:0,retain:false},err=>{if(err){console.error("Publish lỗi:",err);alert("Không thể gửi lệnh MQTT!");return}console.log("MQTT TX:",topic,payload)});return true}

/* ===================== ĐIỀU KHIỂN: LÀM MỜ + KHÓA KHI NGUỒN OFF ===================== */
function setControlsLocked(locked){
  // Mục 1 (POWER) luôn dùng được; mục 2..5 vẫn hiện nhưng bị làm mờ + khóa khi nguồn OFF
  document.querySelectorAll(".control-list .control-item").forEach((item,index)=>{
    if(index===0)return;
    item.classList.toggle("is-locked",locked);
    item.querySelectorAll("button").forEach(btn=>{btn.disabled=locked});
  });
  setText("controlNote",locked?"Bật nguồn để mở các chế độ điều khiển.":"Lệnh điều khiển sẽ được gửi qua MQTT.");
}

function updateControlUI(){
  const powerButton=getElement("powerToggle");
  const powerState=getElement("powerState");
  if(powerButton){
    powerButton.textContent=deviceState.power?"ON":"OFF";
    powerButton.style.background=deviceState.power?"#34aa76":"#d9555e";
  }
  if(powerState)powerState.textContent=deviceState.power?"Thiết bị bật":"Thiết bị tắt";

  const autoButton=getElement("autoToggle");
  const autoState=getElement("autoState");
  if(autoButton){
    autoButton.textContent=deviceState.auto?"AUTO ON":"AUTO OFF";
    autoButton.style.background=deviceState.auto?"#34aa76":"#486f7f";
  }
  if(autoState)autoState.textContent=deviceState.auto?"Đã bật":"Chưa bật";

  const fill=getElement("fanFill");
  if(fill)fill.style.width=deviceState.fanSpeed+"%";
  setText("fanValue",deviceState.fanSpeed);

  const sleepButton=getElement("sleepToggle");
  const sleepIcon=getElement("sleepIcon");
  const sleepText=getElement("sleepButtonText");
  const sleepState=getElement("sleepState");
  if(sleepIcon)sleepIcon.textContent=deviceState.sleep?"🌙":"☀️";
  if(sleepText)sleepText.textContent=deviceState.sleep?"SLEEP":"DAY";
  if(sleepButton)sleepButton.style.background=deviceState.sleep?"#35445f":"#486f7f";
  if(sleepState)sleepState.textContent=deviceState.sleep?"Đèn đã tắt":"Đèn đang bật";
  setText("timerValue",deviceState.timer+"h");

  setControlsLocked(!deviceState.power);
}

function resetFunctionState(){
  // Nguồn OFF: firmware xóa hết chế độ, web làm giống vậy
  deviceState.auto=false;
  deviceState.fanSpeed=0;
  deviceState.sleep=false;
  deviceState.timer=0;
}

function togglePower(){
  deviceState.power=!deviceState.power;
  if(deviceState.power){
    if(deviceState.fanSpeed<=0)deviceState.fanSpeed=25; // firmware đặt 25% khi bật nguồn
  }else{
    resetFunctionState();
  }
  updateControlUI();
  publishMQTT({power:deviceState.power?"ON":"OFF"});
}

function toggleAuto(){
  if(!deviceState.power)return;          // nguồn OFF: không làm gì
  deviceState.auto=!deviceState.auto;
  updateControlUI();
  publishMQTT({auto:deviceState.auto?"ON":"OFF"});
}

function stepFanSpeed(){
  if(!deviceState.power)return;          // nguồn OFF: không làm gì
  deviceState.fanSpeed+=25;
  if(deviceState.fanSpeed>100)deviceState.fanSpeed=0;
  deviceState.auto=false;                // đổi tay thì thoát Auto (giống firmware)
  updateControlUI();
  publishMQTT({fanSpeed:deviceState.fanSpeed});
}

function toggleSleep(){
  if(!deviceState.power)return;          // nguồn OFF: không làm gì
  deviceState.sleep=!deviceState.sleep;
  updateControlUI();
  publishMQTT({sleep:deviceState.sleep?"ON":"OFF"});
}

function stepTimer(){
  if(!deviceState.power)return;          // nguồn OFF: không làm gì
  deviceState.timer+=1;
  if(deviceState.timer>5)deviceState.timer=0;
  updateControlUI();
  publishMQTT({timer:deviceState.timer});
}

function handleDeviceFeedback(data){
  if(data.power!==undefined){
    const value=String(data.power).toUpperCase();
    deviceState.power=value==="ON"||value==="1"||data.power===true;
  }
  if(data.auto!==undefined){
    const value=String(data.auto).toUpperCase();
    deviceState.auto=value==="ON"||value==="1"||data.auto===true;
  }
  if(data.fanSpeed!==undefined){
    const speed=toValidNumber(data.fanSpeed);
    if(speed!==null)deviceState.fanSpeed=Math.max(0,Math.min(100,speed));
  }
  if(data.speed!==undefined){
    const speed=toValidNumber(data.speed);
    if(speed!==null)deviceState.fanSpeed=Math.max(0,Math.min(100,speed));
  }
  if(data.sleep!==undefined){
    const value=String(data.sleep).toUpperCase();
    deviceState.sleep=value==="ON"||value==="1"||data.sleep===true;
  }
  if(data.timer!==undefined){
    const timer=toValidNumber(data.timer);
    if(timer!==null)deviceState.timer=Math.max(0,Math.min(5,timer));
  }
  if(data.fan!==undefined){
    const fan=String(data.fan).toUpperCase();
    if(fan==="ON"){
      deviceState.power=true;
      if(deviceState.fanSpeed===0)deviceState.fanSpeed=25;
    }else if(fan==="OFF"){
      deviceState.fanSpeed=0;
    }
  }
  // Thiết bị báo OFF (bấm nút trên máy, hết hẹn giờ...): xóa chế độ và khóa điều khiển
  if(!deviceState.power)resetFunctionState();
  updateControlUI();
}
/* ===================== HẾT PHẦN ĐIỀU KHIỂN ===================== */

function checkLiveStatus(){if(!lastSensorTime)return;const elapsed=Date.now()-lastSensorTime.getTime();if(elapsed>60000){const badge=getElement("pmBadge");if(badge){badge.textContent="OFFLINE";badge.style.background="#ffe0e3";badge.style.color="#c93d49"}}}
function setupEventListeners(){const subscribe=getElement("btnSubscribe");if(subscribe)subscribe.addEventListener("click",subscribeTopic);const unsubscribe=getElement("btnUnsubscribe");if(unsubscribe)unsubscribe.addEventListener("click",unsubscribeTopic);const power=getElement("powerToggle");if(power)power.addEventListener("click",togglePower);const auto=getElement("autoToggle");if(auto)auto.addEventListener("click",toggleAuto);const fan=getElement("fanStepBtn");if(fan)fan.addEventListener("click",stepFanSpeed);const sleep=getElement("sleepToggle");if(sleep)sleep.addEventListener("click",toggleSleep);const timer=getElement("timerBtn");if(timer)timer.addEventListener("click",stepTimer)}
window.addEventListener("load",()=>{initializeChart();setupEventListeners();updateControlUI();connectToBroker();setInterval(checkLiveStatus,5000)});