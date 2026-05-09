import { connect } from 'cloudflare:sockets';
//说明：抛弃了ed配置，不要设置/?ed=2560，自适应ws和xhttp双传输协议，xhttp传输模式选stream-one，xhttp不适合pages部署，只用ws建议pages部署，互不影响，本人不使用威廉的反代，需要使用的自行添加代码
let 哎呀呀这是我的VL密钥 = "5b75df69-62e0-4f8d-82f4-c4763c6a9ec3"; //建议更改为自己的标准化UUID

let 启用反代功能 = true //选择是否启用反代功能【总开关】，false，true，现在你可以自由的选择是否启用反代功能了
let 反代IP = 'ProxyIP.JP.CMLiussss.net' //反代IP或域名，反代IP端口一般情况下不用填写，如果你非要用非标反代的话，可以填'ts.hpc.tw:443'这样

let 启用SOCKS5反代 = false //如果启用此功能，原始反代将失效，很多S5不一定支持ipv6，启用则需禁用doh查询ipv6功能
let 启用SOCKS5全局反代 = false //选择是否启用SOCKS5全局反代，启用后所有访问都是S5的落地【无论你客户端选什么节点】，访问路径是客户端--CF--SOCKS5，当然启用此功能后延迟=CF+SOCKS5，带宽取决于SOCKS5的带宽，不再享受CF高速和随时满带宽的待遇
let 我的SOCKS5账号 = [
  '@Enkelte_notif:@Notif_Chat@115.91.26.114:2470',
] //格式'账号:密码@地址:端口'，示例admin:admin@127.0.0.1:443或admin:admin@[IPV6]:443，支持无账号密码示例@127.0.0.1:443
//////////////////////////////////////////////////////////////////////////主要架构////////////////////////////////////////////////////////////////////////
export default {
  async fetch(访问请求) {
    const 读取路径 = decodeURIComponent(访问请求.url.replace(/^https?:\/\/[^/]+/, ''));
    const 取参数 = (key) => 读取路径.match(new RegExp(`(?:^|[/?&])${key}=([^&/]+)`))?.[1];
    const 解析布尔 = (值, 默认) => ({ true: true, false: false }[值] ?? 默认);
    反代IP = 取参数('proxyip') || 反代IP;
    const SOCKS5新账号 = 取参数('socks5');
    我的SOCKS5账号 = [...(SOCKS5新账号 ? [SOCKS5新账号] : []), ...我的SOCKS5账号];
    启用SOCKS5反代 = 解析布尔(取参数('socks5-open'), 启用SOCKS5反代);
    启用SOCKS5全局反代 = 解析布尔(取参数('socks5-global'), 启用SOCKS5全局反代);
    if (访问请求.headers.get('Upgrade') === 'websocket'){
      const [客户端, WS接口] = Object.values(new WebSocketPair());
      WS接口.accept();
      WS接口.binaryType = "arraybuffer";
      处理数据(WS接口, true);
      return new Response(null, { status: 101, webSocket: 客户端 }); //一切准备就绪后，回复客户端WS连接升级成功
    } else if (访问请求.method === 'POST' && 访问请求.body) {
      return await 处理数据(访问请求, false);
    } else {
      return new Response('Hello World!', { status: 200 });
    }
  }
};
async function 处理数据(数据接口, 传输协议, 发送数据, 传输队列 = Promise.resolve()) {
  if (传输协议) {
    处理WS流();
  } else {
    return await 处理XHTTP流();
  }
  async function 处理WS流(是首包 = true, 处理首包数据 = Promise.resolve()) {
    数据接口.addEventListener('message', async event => {
      await 传输队列;
      if (是首包) {
        是首包 = false;
        处理首包数据 = 处理首包数据.then(async () => await 处理首包(event.data)).catch(e => {throw (e)});
      } else {
        await 处理首包数据;
        传输队列 = 传输队列.then(async () => {try { await 发送数据.write(event.data) } catch {}}).catch(e => {throw (e)});
      }
    });
    async function 处理首包 (首包数据) {
      const 解析首包 = await 解析首包数据(new Uint8Array(首包数据));
      发送数据 = 解析首包.TCP接口.writable.getWriter();
      if (解析首包.是DNS) {
        数据接口.send(解析首包.初始数据);
        return;
      }
      await 发送数据.write(解析首包.初始数据);
      数据回传通道(解析首包.TCP接口, 解析首包.版本号).pipeTo(new WritableStream({ write(数据) { 数据接口.send(数据) } }));
    }
  }
  async function 处理XHTTP流() {
    const 读取器 = 数据接口.body.getReader();
    const 请求数据 = (await 读取器.read()).value;
    const 解析首包 = await 解析首包数据(new Uint8Array(请求数据));
    if (解析首包.是DNS) return new Response(解析首包.初始数据);
    发送数据 = 解析首包.TCP接口.writable.getWriter();
    await 发送数据.write(解析首包.初始数据);
    数据发送通道(读取器);
    return new Response(数据回传通道(解析首包.TCP接口, 解析首包.版本号));
    async function 数据发送通道(读取器) {
      while (true) {
        await 传输队列;
        const { done: 流结束, value: 请求数据 } = await 读取器.read();
        if (流结束) break;
        if(请求数据?.length > 0) 传输队列 = 传输队列.then(async () => {try { await 发送数据.write(请求数据) } catch {}}).catch(e => {throw (e)});
      }
    }
  }
  function 数据回传通道 (TCP接口, 版本号) {
    const 读取管道 = new TransformStream({
      async start(控制器) { 
        控制器.enqueue(new Uint8Array([版本号, 0]));
      },
      transform(返回数据, 控制器) { 传输队列 = 传输队列.then(() => 控制器.enqueue(返回数据)).catch(e => {throw (e)}) }
    });
    TCP接口.readable.pipeTo(读取管道.writable);
    return 读取管道.readable;
  }
}
async function 解析首包数据(二进制数据) {
  let 识别地址类型, 访问地址, 地址长度;
  if (二进制数据.length < 32) throw new Error('数据长度不足');
  const 获取协议头 = 二进制数据[0];
  const 验证VL的密钥 = (a, i = 0) => [...a.slice(i, i + 16)].map(b => b.toString(16).padStart(2, '0')).join('').replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  if (验证VL的密钥(二进制数据.slice(1, 17)) !== 哎呀呀这是我的VL密钥) throw new Error('UUID验证失败');
  const 提取端口索引 = 18 + 二进制数据[17] + 1;
  const 访问端口 = new DataView(二进制数据.buffer, 提取端口索引, 2).getUint16(0);
  if (访问端口 === 53) { //这个处理是应对某些客户端优先强制查询dns的情况，通过加密通道udp over tcp的
    const 提取DNS查询报文 = 二进制数据.slice(提取端口索引 + 9);
    const 查询DOH结果 = await fetch('https://1.1.1.1/dns-query', {
      method: 'POST',
      headers: {
        'content-type': 'application/dns-message',
      },
      body: 提取DNS查询报文
    })
    const 提取DOH结果 = await 查询DOH结果.arrayBuffer();
    const 构建长度头部 = new Uint8Array([(提取DOH结果.byteLength >> 8) & 0xff, 提取DOH结果.byteLength & 0xff]);
    const 拼接DNS结果 = await new Blob([构建长度头部, 提取DOH结果]);
    return { 初始数据: 拼接DNS结果, 是DNS: true }
  }
  const 提取地址索引 = 提取端口索引 + 2;
  识别地址类型 = 二进制数据[提取地址索引];
  let 地址信息索引 = 提取地址索引 + 1;
  switch (识别地址类型) {
    case 1:
      地址长度 = 4;
      访问地址 = 二进制数据.slice(地址信息索引, 地址信息索引 + 地址长度).join('.');
      break;
    case 2:
      地址长度 = 二进制数据[地址信息索引];
      地址信息索引 += 1;
      访问地址 = new TextDecoder().decode(二进制数据.slice(地址信息索引, 地址信息索引 + 地址长度));
      break;
    case 3:
      地址长度 = 16;
      const ipv6 = [];
      const 读取IPV6地址 = new DataView(二进制数据.buffer, 地址信息索引, 16);
      for (let i = 0; i < 8; i++) ipv6.push(读取IPV6地址.getUint16(i * 2).toString(16).padStart(4, '0')); //修复了v6地址完全展开，方便s5可直接调用
      访问地址 = ipv6.join(':');
      break;
    default:
      throw new Error ('无效的访问地址');
  }
  const 写入初始数据 = 二进制数据.slice(地址信息索引 + 地址长度);
  const TCP接口 = await 创建TCP接口连接(访问地址, 访问端口, 识别地址类型);
  return { 版本号: 获取协议头, TCP接口: TCP接口, 初始数据: 写入初始数据 };
}
async function 创建TCP接口连接(访问地址, 访问端口, 识别地址类型, TCP接口) {
    if (启用反代功能 && 启用SOCKS5反代 && 启用SOCKS5全局反代) {
      TCP接口 = await 创建SOCKS5接口(识别地址类型, 访问地址, 访问端口);
    } else {
      try {
        const 解析IP = 匹配地址(访问地址);
        if (解析IP.类型 === 'ipv6') 解析IP.地址 = `[${解析IP.地址}]`
        TCP接口 = connect({ hostname: 解析IP.地址, port: 访问端口 });
        await TCP接口.opened;
      } catch {
        if (启用反代功能) {
          if (启用SOCKS5反代) {
            TCP接口 = await 创建SOCKS5接口(识别地址类型, 访问地址, 访问端口);
          } else {
            const 解析反代IP = 匹配地址(反代IP);
            if (解析反代IP.类型 === 'ipv6') 解析反代IP.地址 = `[${解析反代IP.地址}]`
            TCP接口 = connect({ hostname: 解析反代IP.地址, port: 解析反代IP.端口});
          }
        }
      }
    }
  return TCP接口;
}
//////////////////////////////////////////////////////////////////////////SOCKS5部分//////////////////////////////////////////////////////////////////////
async function 创建SOCKS5接口(识别地址类型, 访问地址, 访问端口, 解析SOCKS5, SOCKS5接口, 转换访问地址, 传输数据, 读取数据) {
  let 索引SOCKS5账号 = 0;
  我的SOCKS5账号 = Array.isArray(我的SOCKS5账号) ? 我的SOCKS5账号 : [我的SOCKS5账号];
  while (索引SOCKS5账号 < 我的SOCKS5账号.length) {
    const 提取SOCKS5账号 = 我的SOCKS5账号[索引SOCKS5账号]
    try {
      解析SOCKS5 = await 获取SOCKS5账号(提取SOCKS5账号);
      SOCKS5接口 = connect({ hostname: 解析SOCKS5.地址, port: 解析SOCKS5.端口 });
      await SOCKS5接口.opened;
      传输数据 = SOCKS5接口.writable.getWriter();
      读取数据 = SOCKS5接口.readable.getReader();
      const 转换数组 = new TextEncoder(); //把文本内容转换为字节数组，如账号，密码，域名，方便与S5建立连接
      const 构建S5认证 = new Uint8Array([5, 2, 0, 2]); //构建认证信息,支持无认证和用户名/密码认证
      await 传输数据.write(构建S5认证); //发送认证信息，确认目标是否需要用户名密码认证
      const 读取认证要求 = (await 读取数据.read()).value;
      if (读取认证要求[1] === 0x02) { //检查是否需要用户名/密码认证
        if (!解析SOCKS5.账号 || !解析SOCKS5.密码) {
          throw new Error (`未配置账号密码`);
        }
        const 构建账号密码包 = new Uint8Array([ 1, 解析SOCKS5.账号.length, ...转换数组.encode(解析SOCKS5.账号), 解析SOCKS5.密码.length, ...转换数组.encode(解析SOCKS5.密码) ]); //构建账号密码数据包，把字符转换为字节数组
        await 传输数据.write(构建账号密码包); //发送账号密码认证信息
        const 读取账号密码认证结果 = (await 读取数据.read()).value;
        if (读取账号密码认证结果[0] !== 0x01 || 读取账号密码认证结果[1] !== 0x00) { //检查账号密码认证结果，认证失败则退出
          throw new Error (`账号密码错误`);
        }
      }
      switch (识别地址类型) {
        case 1: // IPv4
          转换访问地址 = new Uint8Array( [1, ...访问地址.split('.').map(Number)] );
          break;
        case 2: // 域名
          转换访问地址 = new Uint8Array( [3, 访问地址.length, ...转换数组.encode(访问地址)] );
          break;
        case 3: // IPv6
          转换访问地址 = new Uint8Array( [4, ...访问地址.split(':').flatMap(s => [(parseInt(s, 16) >> 8) & 255, parseInt(s, 16) & 255])] );
          break;
      }
      const 构建转换后的访问地址 = new Uint8Array([ 5, 1, 0, ...转换访问地址, 访问端口 >> 8, 访问端口 & 0xff ]); //构建转换好的地址消息
      await 传输数据.write(构建转换后的访问地址); //发送转换后的地址
      const 检查返回响应 = (await 读取数据.read()).value;
      if (检查返回响应[0] !== 0x05 || 检查返回响应[1] !== 0x00) {
        throw new Error (`目标地址连接失败，访问地址: ${访问地址}，地址类型: ${识别地址类型}`);
      }
      传输数据.releaseLock();
      读取数据.releaseLock();
      return SOCKS5接口;
    } catch {
      索引SOCKS5账号++
    };
  }
  传输数据?.releaseLock();
  读取数据?.releaseLock();
  await SOCKS5接口?.close();
  throw new Error (`所有SOCKS5账号失效`);
}
async function 获取SOCKS5账号(SOCKS5) {
  const 分隔账号 = SOCKS5.includes("@") ? SOCKS5.lastIndexOf("@") : -1;
  const 账号段 = SOCKS5.slice(0, 分隔账号);
  const 地址段 = 分隔账号 !== -1 ? SOCKS5.slice(分隔账号 + 1) : SOCKS5;
  const [账号, 密码] = [账号段.slice(0, 账号段.lastIndexOf(":")), 账号段.slice(账号段.lastIndexOf(":") + 1)];
  const 解析SOCKS5地址 = 匹配地址(地址段);
  if (解析SOCKS5地址.类型 === 'ipv6') 解析SOCKS5地址.地址 = `[${解析SOCKS5地址.地址}]`
  return { 账号: 账号, 密码: 密码, 地址: 解析SOCKS5地址.地址 , 端口: 解析SOCKS5地址.端口 };
}
function 匹配地址(地址) {
  const 匹配 = 地址.match(/^(?:\[(?<ipv6>(?!fc00:)(?!fd00:)(?!fe80:)(?!::1)(?!0:)[0-9a-fA-F:]+)\]|(?<ipv6>(?!fc00:)(?!fd00:)(?!fe80:)(?!::1)(?!0:)[0-9a-fA-F:]+)|(?<ipv4>(?!10\.)(?!127\.)(?!169\.254\.)(?!172\.(1[6-9]|2\d|3[0-1])\.)(?!192\.168\.)(?!0\.)\d{1,3}(?:\.\d{1,3}){3})|(?<domain>[a-zA-Z0-9.-]+))(?::(?<port>\d+))?$/);  
  const { ipv6, ipv4, domain, port } = 匹配.groups;
  function 展开IPv6(ip) {
    ip = ip.replace(/^\[|\]$/g, '');
    if (ip.includes('::')) {
      const [前, 后] = ip.split('::');
      const 前段 = 前 ? 前.split(':') : [];
      const 后段 = 后 ? 后.split(':') : [];
      const 缺失数量 = 8 - (前段.length + 后段.length);
      const 填充 = Array(缺失数量).fill('0');
      ip = [...前段, ...填充, ...后段].join(':');
    }
    return ip
      .split(':')
      .map(x => x.padStart(4, '0').toLowerCase())
      .join(':');
  }
  const 展开IPv6地址 = ipv6 ? 展开IPv6(ipv6) : null;
  return {
    类型: ipv6 ? 'ipv6' : ipv4 ? 'ipv4' : '域名',
    地址: 展开IPv6地址 || ipv4 || domain,
    端口: port ? Number(port) : 443
  };
}