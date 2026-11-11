//! Listener setup shared by the production server and the router tests.

use axum::serve::{ListenerExt, TapIo};
use tokio::net::{TcpListener, TcpStream};

pub type NodelayListener = TapIo<TcpListener, fn(&mut TcpStream)>;

fn set_nodelay(stream: &mut TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::warn!("failed to set TCP_NODELAY on client socket: {error}");
    }
}

/// Sets `TCP_NODELAY` on every accepted client socket.
///
/// Without it, Nagle's algorithm and the peer's delayed ACK hold a small SSE
/// write for about 40 ms on loopback. A failure to set the option is logged and
/// the connection is kept. The concrete return type keeps axum's
/// `Connected<IncomingStream<..>>` impl for `SocketAddr` usable.
pub fn nodelay_listener(listener: TcpListener) -> NodelayListener {
    listener.tap_io(set_nodelay as fn(&mut TcpStream))
}
