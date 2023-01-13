//index.js
const io = require('socket.io-client')
const mediasoupClient = require('mediasoup-client')
const {StrictEventEmitter} = require('socket.io-client/build/typed-events');

const roomName = window.location.pathname.split('/')[2]

const socket = io("/mediasoup")

socket.on('connection-success', ({ socketId }) => {
  console.log('🌟SocketId🌟: ' + socketId)
  getLocalStream()
})

let device
let rtpCapabilities
let producerTransport
let consumerTransports = []
let audioProducer
let videoProducer
let consumer
let isProducer = false

let myStream;

// https://mediasoup.org/documentation/v3/mediasoup-client/api/#ProducerOptions
// https://mediasoup.org/documentation/v3/mediasoup-client/api/#transport-produce
let params = {
  // mediasoup params
  encodings: [
    {
      rid: 'r0',
      maxBitrate: 100000,
      scalabilityMode: 'S1T3',
    },
    {
      rid: 'r1',
      maxBitrate: 300000,
      scalabilityMode: 'S1T3',
    },
    {
      rid: 'r2',
      maxBitrate: 900000,
      scalabilityMode: 'S1T3',
    },
  ],
  // https://mediasoup.org/documentation/v3/mediasoup-client/api/#ProducerCodecOptions
  codecOptions: {
    videoGoogleStartBitrate: 1000
  }
}

let audioParams;
let videoParams = {params};
let consumingTransports = [];

const streamSuccess = (stream) => {
  localVideo.srcObject = stream
  myStream = stream;
  audioParams = { track: stream.getAudioTracks()[0], ...audioParams };
  videoParams = { track: stream.getVideoTracks()[0], ...videoParams };

  joinRoom()
}

const joinRoom = () => {
  socket.emit('joinRoom', {roomName}, (data) => {
    console.log(`Router RTP Capabilities... ${data.rtpCapabilities}`)
    // we assign to local variable and will be used when
    // loading the client Device (see createDevice above)
    rtpCapabilities = data.rtpCapabilities

    // once we have rtpCapabilities from the Router, create Device
    createDevice()
  })
}

const getLocalStream = () => {
  navigator.mediaDevices.getUserMedia({
    audio: true,
    video: {
      width: {
        min: 640,
        max: 1920,
      },
      height: {
        min: 400,
        max: 1080,
      }
    }
  })
  .then(streamSuccess)
  .catch(error => {
    console.log(error.message)
  })
}

// A device is an endpoint connecting to a Router on the
// server side to send/recive media
const createDevice = async () => {
  try {
    device = new mediasoupClient.Device()

    // https://mediasoup.org/documentation/v3/mediasoup-client/api/#device-load
    // Loads the device with RTP capabilities of the Router (server side)
    await device.load({
      // see getRtpCapabilities() below
      routerRtpCapabilities: rtpCapabilities
    })

    console.log('Device RTP Capabilities', device.rtpCapabilities)

    // once the device loads, create transport
    createSendTransport()

  } catch (error) {
    console.log(error)
    if (error.name === 'UnsupportedError')
      console.warn('browser not supported')
  }
}

const createSendTransport = () => {
  // see server's socket.on('createWebRtcTransport', sender?, ...)
  // this is a call from Producer, so sender = true
  socket.emit('createWebRtcTransport', { consumer: false }, ({ params }) => {
    // The server sends back params needed 
    // to create Send Transport on the client side
    if (params.error) {
      console.log(params.error)
      return
    }

    console.log(params)

    // creates a new WebRTC Transport to send media
    // based on the server's producer transport params
    // https://mediasoup.org/documentation/v3/mediasoup-client/api/#TransportOptions
    producerTransport = device.createSendTransport(params)

    // https://mediasoup.org/documentation/v3/communication-between-client-and-server/#producing-media
    // this event is raised when a first call to transport.produce() is made
    // see connectSendTransport() below
    producerTransport.on('connect', async ({ dtlsParameters }, callback, errback) => {
      try {
        // Signal local DTLS parameters to the server side transport
        // see server's socket.on('transport-connect', ...)
        await socket.emit('transport-connect', {
          dtlsParameters,
        })

        // Tell the transport that parameters were transmitted.
        callback()

      } catch (error) {
        errback(error)
      }
    })

    producerTransport.on('produce', async (parameters, callback, errback) => {
      console.log(parameters)

      try {
        // tell the server to create a Producer
        // with the following parameters and produce
        // and expect back a server side producer id
        // see server's socket.on('transport-produce', ...)
        await socket.emit('transport-produce', {
          kind: parameters.kind,
          rtpParameters: parameters.rtpParameters,
          appData: parameters.appData,
        }, ({ id, producersExist }) => {
          // Tell the transport that parameters were transmitted and provide it with the
          // server side producer's id.
          callback({ id })

          // if producers exist, then join room
          if (producersExist) getProducers()
        })
      } catch (error) {
        errback(error)
      }
    })

    connectSendTransport()
  })
}

const connectSendTransport = async () => {
  // we now call produce() to instruct the producer transport
  // to send media to the Router
  // https://mediasoup.org/documentation/v3/mediasoup-client/api/#transport-produce
  // this action will trigger the 'connect' and 'produce' events above
  
  audioProducer = await producerTransport.produce(audioParams);
  videoProducer = await producerTransport.produce(videoParams);

  audioProducer.on('trackended', () => {
    console.log('audio track ended')

    // close audio track
  })

  audioProducer.on('transportclose', () => {
    console.log('audio transport ended')

    // close audio track
  })
  
  videoProducer.on('trackended', () => {
    console.log('video track ended')

    // close video track
  })

  videoProducer.on('transportclose', () => {
    console.log('video transport ended')

    // close video track
  })
}

const signalNewConsumerTransport = async (remoteProducerId, socketId) => {
  //check if we are already consuming the remoteProducerId
  if (consumingTransports.includes(remoteProducerId)) return;
  consumingTransports.push(remoteProducerId);

  await socket.emit('createWebRtcTransport', { consumer: true }, ({ params }) => {
    // The server sends back params needed 
    // to create Send Transport on the client side
    if (params.error) {
      console.log(params.error)
      return
    }
    console.log(`PARAMS... ${params}`)

    let consumerTransport
    try {
      consumerTransport = device.createRecvTransport(params)
    } catch (error) {
      // exceptions: 
      // {InvalidStateError} if not loaded
      // {TypeError} if wrong arguments.
      console.log(error)
      return
    }

    consumerTransport.on('connect', async ({ dtlsParameters }, callback, errback) => {
      try {
        // Signal local DTLS parameters to the server side transport
        // see server's socket.on('transport-recv-connect', ...)
        await socket.emit('transport-recv-connect', {
          dtlsParameters,
          serverConsumerTransportId: params.id,
        })

        // Tell the transport that parameters were transmitted.
        callback()
      } catch (error) {
        // Tell the transport that something was wrong
        errback(error)
      }
    })

    connectRecvTransport(consumerTransport, remoteProducerId, params.id, socketId)
  })
}

// server informs the client of a new producer just joined

socket.on('new-producer', ({ producerId, socketId }) => signalNewConsumerTransport(producerId, socketId))

const getProducers = () => {
  socket.emit('getProducers', producerIds => {
    console.log(producerIds)
    // for each of the producer create a consumer
    // producerIds.forEach(id => signalNewConsumerTransport(id))
    producerIds.forEach(signalNewConsumerTransport)
  })
}

const connectRecvTransport = async (consumerTransport, remoteProducerId, serverConsumerTransportId, socketId) => {
  // for consumer, we need to tell the server first
  // to create a consumer based on the rtpCapabilities and consume
  // if the router can consume, it will send back a set of params as below
  await socket.emit('consume', {
    rtpCapabilities: device.rtpCapabilities,
    remoteProducerId,
    serverConsumerTransportId,
  }, async ({ params }) => {
    if (params.error) {
      console.log('Cannot Consume')
      return
    }

    console.log(`Consumer Params ${params}`)
    // then consume with the local consumer transport
    // which creates a consumer
    const consumer = await consumerTransport.consume({
      id: params.id,
      producerId: params.producerId,
      kind: params.kind,
      rtpParameters: params.rtpParameters
    })

    consumerTransports = [
      ...consumerTransports,
      {
        consumerTransport,
        serverConsumerTransportId: params.id,
        producerId: remoteProducerId,
        consumer,
      },
    ]

  const videoContainer = document.getElementById("videoContainer")
    let videoDiv = document.getElementById(socketId)
      if (!videoDiv) {
        const newElem = document.createElement('div')
        newElem.setAttribute('id', socketId)
        newElem.setAttribute('class', 'remoteVideo')
        videoContainer.appendChild(newElem)
        videoDiv = newElem
      }

        if (params.kind === 'audio') {
            videoDiv.innerHTML += '<audio autoplay id="' + remoteProducerId + '" off="false"></audio>'
        } else {
            videoDiv.innerHTML += '<video autoplay id="' + remoteProducerId + '" class="video" off="false"></video>'
            const wrapDiv = document.createElement('div')

            const audioOut = document.createElement('button')
            const audioOutI = document.createElement('i')
            audioOutI.setAttribute('class', 'fa-solid fa-microphone audio-icon')
            audioOut.appendChild(audioOutI)

            const videoOut = document.createElement('button')
            const videoOutI = document.createElement('i')
            videoOutI.setAttribute('class', 'fa-solid fa-video video-icon')
            videoOut.appendChild(videoOutI)

            wrapDiv.appendChild(audioOut)
            wrapDiv.appendChild(videoOut)

            videoOut.addEventListener('click', (event) => toggleWebRTCContext(event, 'video'));
            audioOut.addEventListener('click', (event) => toggleWebRTCContext(event, 'audio'));

            videoDiv.appendChild(wrapDiv)
        }
    // destructure and retrieve the video track from the producer
    const { track } = consumer

    document.getElementById(remoteProducerId).srcObject = new MediaStream([track])

    // the server consumer started with media paused
    // so we need to inform the server to resume
    socket.emit('consumer-resume', { serverConsumerId: params.serverConsumerId })
  })
}

socket.on('student-video-controller', ({off}) => {
  myStream
  .getVideoTracks()
  .forEach((track) => (track.enabled = !track.enabled)); // 카메라 화면 요소를 키고 끄기 
})

socket.on('student-audio-controller', ({off}) => {
  myStream
  .getAudioTracks()
  .forEach((track) => (track.enabled = !track.enabled)); // 카메라 화면 요소를 키고 끄기 
})

socket.on('producer-closed', ({ remoteProducerId }) => {
  // server notification is received when a producer is closed
  // we need to close the client-side consumer and associated transport
  const producerToClose = consumerTransports.find(transportData => transportData.producerId === remoteProducerId)
  producerToClose.consumerTransport.close()
  producerToClose.consumer.close()

  // remove the consumer transport from the list
  consumerTransports = consumerTransports.filter(transportData => transportData.producerId !== remoteProducerId)

  // remove the video div element
  // 🌟1. 원본코드
  // videoContainer.removeChild(document.getElementById(`td-${remoteProducerId}`))

  // //🌟 수정코드
  const videoContainer = document.getElementById("videoContainer");
  const removeTarget = document.getElementById(remoteProducerId);
  if(removeTarget){
    videoContainer.removeChild(removeTarget)
  }
})

//🌟 수정코드 togggleWebRTCCONTEXT
function toggleWebRTCContext(event, context) {

  console.log('🍀🍀event🍀🍀'+ event)
  console.log('🍀🍀context🍀🍀' + context)

  const remoteVideoDiv = event.target.closest(".remoteVideo")
  console.log(remoteVideoDiv)
  const socketId = remoteVideoDiv.id
  const ele = remoteVideoDiv.querySelector(context)
  const off = !JSON.parse(ele.getAttribute("off"))

  ele.setAttribute("off", off)

  const icon = remoteVideoDiv.querySelector(`.${context}-icon`)
  const toggleIconName = context === 'video' ? 'video' : 'microphone'
  icon.classList.toggle(`fa-${toggleIconName}`, !off);
  icon.classList.toggle(`fa-${toggleIconName}-slash`, off);

  console.log((off ? `${context} on` : `${context} off`) + " target socket id: ", socketId)

  // 'video-out', 'audio-out' emit 
  socket.emit(`${context}-out`, {
      socketId: socketId,
      off: off
  })
}

const muteBtn = document.getElementById("mute"); 
const muteIcon = document.getElementById("muteIcon"); 
const cameraBtn = document.getElementById("camera");
const cameraIcon = document.getElementById("cameraIcon");
let muted = false;
let cameraOff = false;

function handleMuteClick() {
  myStream
  .getAudioTracks()
  .forEach((track) => (track.enabled = !track.enabled)); // 오디오 요소를 키고 끄기
  if (!muted) { // mute가 아닌 상태라면 (초기 상태)
    // muteBtn.innerText = "Unmute";
    muted = true;
    muteIcon.classList.remove('fa-microphone')
    muteIcon.classList.add('fa-microphone-slash')

  } else {
    // muteBtn.innerText = "Mute";
    muted = false;
    muteIcon.classList.remove('fa-microphone-slash')
    muteIcon.classList.add('fa-microphone')
  }
}

function handleCameraClick() {
  myStream
  .getVideoTracks()
  .forEach((track) => (track.enabled = !track.enabled)); // 카메라 화면 요소를 키고 끄기 
  if (!cameraOff) { // 카메라가 켜진 상태라면 (초기 상태)
    cameraOff = true;
    cameraIcon.classList.remove('fa-video');
    cameraIcon.classList.add('fa-video-slash');

  } else {
    cameraOff = false;
    cameraIcon.classList.remove('fa-video-slash');
    cameraIcon.classList.add('fa-video');
  }
}


muteBtn.addEventListener("click", handleMuteClick);
cameraBtn.addEventListener("click", handleCameraClick);