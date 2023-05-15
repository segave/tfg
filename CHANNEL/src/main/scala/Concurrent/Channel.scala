package Concurrent

class Channel[A] {
  class LinkedList[A]{
    var elem: A = _
    var next: LinkedList[A] = null
  }
  private var written = new LinkedList[A] // FIFO buffer, implementado a través
  private var lastWritten = written       // de alias de un linked list
  private var nreaders = 0
  def write(x: A) = synchronized {
    lastWritten.elem = x
    lastWritten.next = new LinkedList[A]
    lastWritten = lastWritten.next
    if (nreaders > 0) notify()
  }
  def read: A = synchronized {
    while (written.next == null) {
      try {
        nreaders += 1
        wait()
      }
      finally nreaders -= 1
    }
    val x = written.elem
    written = written.next
    x
  }
}