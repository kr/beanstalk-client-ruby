Beanstalk Ruby Client
=====================

Beanstalk is a simple, fast work queue. Its interface is generic, but was
originally designed for reducing the latency of page views in high-volume web
applications by running time-consuming tasks asynchronously.

For more information, see:

 - <http://kr.github.com/beanstalkd/>
 - <http://github.com/kr/beanstalkd/raw/master/doc/protocol.txt>

## Notes

If you want to use this library from multiple concurrent threads, you should
synchronize access to the connection. This library does no internal
synchronization.

## Contributors

 - Isaac Feliu
 - Peter Kieltyka
 - Martyn Loughran
 - Dustin Sallings
