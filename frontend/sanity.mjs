import http from 'http';
const port = Number(process.env.PORT||5000);
http.createServer((_,res)=>res.end('ok')).listen(port,'0.0.0.0',()=>console.log('listening',port));
