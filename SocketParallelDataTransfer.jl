"""
   SocketParallelDataTransfer

Module enabling inter-process communication using TCP sockets. The enviroment
is initiated by executing setupEnvironment function. For indicated process
a message register is created containing a set of Array of chosen type and fixed size.
The values of the process' register can be set using setRegisterValue. Finally,
the a messages located in the given process can be downloaded using fetchFrom.

"""

module SocketParallelDataTransfer
   using Distributed,Sockets, StaticArrays
   _messageRegister= Any[]
   _messageTypes = Tuple{DataType,Int64}[]
   _pids = Int64[]

   _servers = Dict{Int, TCPSocket}()
   _clients = Dict{Int, TCPSocket}()

   function _setupRegister(message_types::Array{Tuple{DataType,Int64},1},use_MArrays::Bool)
      global _messageRegister
      global _messageTypes
      global _useMArrays
      _messageRegister = Any[]
      _messageTypes = message_types
      _useMArrays = use_MArrays
      for i in 1:length(message_types)
         push!(_messageRegister, nothing)
      end
   end

   function _resetEnvironment()
      global _messageRegister= Any[]
      global _messageTypes = Tuple{DataType,Int64}[]
      global _pids = Int64[]
      global _servers
      global _clients

      for sock in values(_servers)
         close(sock)
      end

      for sock in values(_clients)
         close(sock)
      end


      _servers = Dict{Int, TCPSocket}()
      _clients = Dict{Int, TCPSocket}()
   end


   function _runServer(proc_id::Int; port = 0, use_ip=false)
      sockInit = false
      if use_ip
         serv_ip = getipaddr()
         server = listen(serv_ip,port)
      else
         serv_ip = nothing
         server = listen(port)
      end


      if port == 0
         port = getsockname(server)[2]
      end
      socket = nothing



      global _useMArrays


      @async begin
         while true
            if !sockInit
               socket = accept(server)
               global _servers[proc_id]=socket
               sockInit = true
            end
            cord = convert(Array{Int,1},reinterpret(Int,read(socket,sizeof(Int))))[1]
            try
               msg = _messageRegister[cord]
               if msg == nothing
                  throw(UndefRefError)
               end
               msg_len = _messageTypes[cord][2]
               msg_to_send = _useMArrays || (length(msg) == msg_len) ? msg : msg[1:msg_len]
               write(socket,1)
               write(socket,msg_to_send)
            catch
               write(socket,0)

            end
         end
      end
      return (serv_ip,port)
   end


   function _setupClient(proc_id::Int,ip,port)
      if ip == nothing
         client = connect(port)
      else
         client = connect(ip,port)
      end
      global _clients[proc_id] = client
   end

   """
      fetchFrom(proc_id::Int,msg_no::Int)

   Fetches a message with number msg_no from a process with pid = proc_id.
   Returns an Array/MArray stored under given position in the process or nothing (if message not found)
   """
   function fetchFrom(proc_id::Int,msg_no::Int)
      global _useMArrays
      client = _clients[proc_id]
      type,len = _messageTypes[msg_no]
      tmp_size = sizeof(type)*len
      write(client,msg_no)
      message_found = convert(Array{Int,1},reinterpret(Int,read(client,sizeof(Int))))[1]
      if message_found == 1
         msg = read(client,tmp_size)
         if _useMArrays
            msg =convert(MArray{Tuple{len},type,1,len},reinterpret(type,msg))
         else

            msg =convert(Array{type},reinterpret(type,msg))

         end
         return msg
      else
         return nothing
      end
   end


   """
      setRegisterValue(x,val::Array)

   Saves the provided Array (val) in the message register under the position x.
   Returns true if the operation was successful - the Array's type and length was
   the same as the x-th entry of the message register. In other case the function
   returns false
   """

   function setRegisterValue(x,val::Array)
      global _useMArrays
      try
         msg_type,msg_size = _messageTypes[x]

         if typeof(val[1]) == msg_type && length(val) == msg_size
            global _messageRegister[x] = _useMArrays ? MArray{Tuple{msg_size}}(val) : val
            return true
         end
      catch
      end
      return false
   end

   """
      setRegisterValue(x,val::MArray)

   Saves the provided Array (val) in the message register under the position x.
   Returns true if the operation was successful - the Array's type and length was
   the same as the x-th entry of the message register. In other case the function
   returns false
   """
   function setRegisterValue(x,val::MArray)
      try
         msg_type,msg_size = _messageTypes[x]

         if typeof(val[1]) == msg_type && length(val) == msg_size
            global _messageRegister[x] = val
            return true
         end
      catch
      end
      return false
   end

   """
      setupEnvironment(proc_ids::Array{Int64,1},
      message_types::Array{Tuple{DataType,Int64},1}[,use_MArrays, ports, use_ip])

   Initiates the setup of the inter-process communication enviroment for pid
   provided in the proc_ids list. In each process from the list a message register
   will be created. The types of stored messages are described by Message_types
   argument containing a list of (Type,Length). By default all messages are
   created as MArrays. They can be stored as Arrays by setting use_MArrays = false

   The function will setup socket connections using port provided automatically
   by operating system. The port range can be changed to chosen list by setting
   ports argument. If not all pid are related to the same server the use_ip argument
   should be set to true
   """

   function setupEnvironment(proc_ids::Array{Int64,1},
      message_types::Array{Tuple{DataType,Int64},1}; use_MArrays=true,
      ports = nothing, use_ip= false)

      if ports != nothing
         if length(proc_ids)*(length(proc_ids)-1)/2 > length(ports)
            throw(ArgumentError("Too few port numbers provided"))
         end
         portsAvailable = copy(ports)
      end

      global _pids = proc_ids
       for pid in proc_ids
         remotecall_fetch(_setupRegister,pid,message_types,use_MArrays)
               for target_pid in filter(p->p!=pid,proc_ids)
                  if ports != nothing
                     server_ip, port = remotecall_fetch(_runServer,target_pid,pid, port = pop!(portsAvailable), use_ip = use_ip)
                  else
                     server_ip, port = remotecall_fetch(_runServer,target_pid,pid, use_ip = use_ip)
                  end
                  remotecall_fetch(_setupClient,pid,target_pid,server_ip,port)

               end

       end

   end
   """
      resetEnvironment()

      Closes all connections created in the enviroment and removes message register
   """
   function resetEnvironment()
      global _pids
      for pid in _pids
        remotecall_fetch(_resetEnvironment,pid)
      end
   end

   export setupEnvironment,fetchFrom,sendTo,resetEnvironment

end #module
