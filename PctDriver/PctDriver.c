/*


Copyright (c) 2008 Slava Imameev, All Rights Reserved

Revision history:
02.09.2008 ( September ) First version

No any part of this code can be used in a commercial project.

The features of the driver
  - user buffers must be aligned on the sector boundary
  - driver supports asynchronous IO ( overlapped IO )

All functions, except the load and unload ones, are prefixed by the 'Pct' string.
The code was made as simple as possible, so there are a lot of opportunites for 
improvements.

*/

#include "PctDriver.h"

//-----------------------------------------------------------------

typedef struct _PCT_USER_REQUEST{

    //
    // the ListEntry is used to anchor deferred request
    //
    LIST_ENTRY       ListEntry;

    //
    // original user Irp
    //
    PIRP             Irp;

    //
    // the locked user buffer
    //
    PMDL             UserBufferMdl;
    PVOID            BufferSystemAddress;

    LARGE_INTEGER    Offset;//in bytes
    ULONG            Size;//in bytes

    PPCT_IO_REQUEST  OriginalUserRequest;

    ULONG            ReadOperation: 0x1;
    ULONG            WriteOperation: 0x1;
    ULONG            PendingReturned: 0x1;

} PCT_USER_REQUEST, *PPCT_USER_REQUEST;


typedef struct _PCT_FILE_FS_CONTEXT{

    PFILE_OBJECT      DiskFileObject;
    DISK_GEOMETRY     DiskGeometry;

} PCT_FILE_FS_CONTEXT, *PPCT_FILE_FS_CONTEXT;

//-----------------------------------------------------------------

#define PCT_MEMORY_TAG 'TtcP'

#define MAX_NUMBER_OF_WORKER_THREADS    16 // can't exceed MAXIMUM_WAIT_OBJECTS!

#if DBG
#define PCT_INVALID_POINTER_VALUE  ((ULONG_PTR)0x1)
#define PCT_IS_POINTER_VALID( _Ptr_ )  ( PCT_INVALID_POINTER_VALUE != ((ULONG_PTR)(_Ptr_)) && NULL != (PVOID)(_Ptr_) )
#define PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( _Ptr_ )  ( *(PULONG_PTR)&(_Ptr_) = PCT_INVALID_POINTER_VALUE );
#else//DBG
#define PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( _Ptr_ ) 
#endif//DBG

static PDRIVER_OBJECT    g_DriverObject;

//
// the object used to send request to the driver
//
static PDEVICE_OBJECT    g_CommunicationDeviceObject;

//
// the event is set in a signal state to wake up worker thread
//
static KEVENT            g_WorkerThreadsPoolEvent;

//
// the event is used to wake up all pool threads and then stop them
//
static KEVENT            g_WorkerThreadsPoolStopEvent;

//
// the queue contains requests to be processed in a worker thread
//
static LIST_ENTRY        g_WorkerThreadsPoolInputQueueHead;

//
// the array contains the pointers to worker thread objects,
// the number of non NULL entries depends on the number of CPUs
//
static PETHREAD          g_WorkerThreadsPool[ MAX_NUMBER_OF_WORKER_THREADS ];

//
// the wait blocks array is used in KeWaitForMultipleObjects to wait until threads stopped
//
static KWAIT_BLOCK       g_WorkerThreadsPoolWaitBlock[ MAX_NUMBER_OF_WORKER_THREADS ];

//
// number of worker threads in the pool
//
static ULONG             g_NumberOfWorkerThreads;

//
// the spin lock protects the threads pool queue
//
static KSPIN_LOCK        g_WorkerThreadsPoolQueueLock;

//-----------------------------------------------------------------

static
VOID 
DriverUnload(
    __in PDRIVER_OBJECT DriverObject
    );

static
NTSTATUS
PctCreateCommunicationDeviceObject();

static
VOID
PctRemoveCommunicationDeviceObject();

static
ULONG
PstNumberOfProcessorUnits();

static
VOID
PctStopWorkerThreadsPool();

static
VOID
PctWorkerThreadRoutine(
    __in_opt PVOID    Context
    );

static
NTSTATUS
PctProcessUserReadWriteRequest(
    __in PPCT_USER_REQUEST UserRequest
    );

static
NTSTATUS
PctDiskReadWriteCompletionRoutine(
    __in PDEVICE_OBJECT  DeviceObject,
    __in PIRP  Irp,
    __in PVOID  Context// actually PPCT_USER_REQUEST
    );

static
NTSTATUS
PctStartWorkerThreadsPool();

static
NTSTATUS
PctGetDriveGeometry( 
    __in PDEVICE_OBJECT    DiskDeviceObject, 
    __out DISK_GEOMETRY*   PtrDiskGeometry 
    );

static
NTSTATUS
PctDeviceIoctlDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    );

static
NTSTATUS
PctCreateDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    );

static
NTSTATUS
PctCloseDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    );

static
NTSTATUS
PctCleanupDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    );

//
// not defined in WDK
//
NTSYSAPI
NTSTATUS
NTAPI
ZwQuerySystemInformation (
    IN SYSTEM_INFORMATION_CLASS SystemInformationClass,
    OUT PVOID                   SystemInformation,
    IN ULONG                    Length,
    OUT PULONG                  ReturnLength
);

//
// ZwWaitForSingleObject is not declared in the ntddk.h file
//
NTSYSAPI
NTSTATUS
NTAPI
ZwWaitForSingleObject (
    IN HANDLE           Handle,
    IN BOOLEAN          Alertable,
    IN PLARGE_INTEGER   Timeout OPTIONAL
);

//
// declared in ntifs.h
//
PDEVICE_OBJECT
IoGetAttachedDevice(
    IN PDEVICE_OBJECT  DeviceObject
    ); 

//
// declared in ntifs.h
//
BOOLEAN
IoIsOperationSynchronous(
    IN PIRP  Irp
    );

//-----------------------------------------------------------------

NTSTATUS 
DriverEntry(
    __in PDRIVER_OBJECT DriverObject,
    __in PUNICODE_STRING RegistryPath
    )
/*++

The entry function, called by the system after the driver 
has been mapped into the system address space

--*/
{
    NTSTATUS    RC;

    g_DriverObject = DriverObject;

    //
    // create a communication device object
    //
    RC = PctCreateCommunicationDeviceObject();
    if( !NT_SUCCESS( RC ) )
        goto __exit;

    //
    // start a worker threads pool
    //
    RC = PctStartWorkerThreadsPool();
    if( !NT_SUCCESS( RC ) )
        goto __exit;

    DriverObject->MajorFunction[ IRP_MJ_CREATE ] = PctCreateDispatch;
    DriverObject->MajorFunction[ IRP_MJ_CLEANUP ] = PctCleanupDispatch;
    DriverObject->MajorFunction[ IRP_MJ_CLOSE ] = PctCloseDispatch;
    DriverObject->MajorFunction[ IRP_MJ_DEVICE_CONTROL ] = PctDeviceIoctlDispatch;

    //
    // make the driver unloadable
    //
    DriverObject->DriverUnload = DriverUnload;

__exit:

    if( !NT_SUCCESS( RC ) )
        DriverUnload( DriverObject );

    return RC;
}

//-----------------------------------------------------------------

VOID 
DriverUnload(
    __in PDRIVER_OBJECT DriverObject
    )
/*++

The unload function, called by the system before unmapping
the driver from the system address space or by the DriverEntry
in case of an error

--*/
{
    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );

    PctRemoveCommunicationDeviceObject();
    PctStopWorkerThreadsPool();

}

//-----------------------------------------------------------------

NTSTATUS
PctCreateCommunicationDeviceObject()
/*++

The function create an object which will be used for the
communication with the driver, the g_CommunicationDeviceObject
will contain the pointer to this object. Also, the function
creates a symbolic link to this object.

The coumterpat function is PctRemoveCommunicationDeviceObject.

--*/
{
    NTSTATUS            RC;
    UNICODE_STRING      CommunicationObjectName;
    UNICODE_STRING      SddlString;

    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );

    RtlInitUnicodeString( &CommunicationObjectName, PCT_FULL_COMM_OBJECT_NAME );

    //
    // Initialize a security descriptor that allows only the System 
    // and Administrators groups to access the communication device.
    // The security descriptor will be stored in 
    // HKLM\SYSTEM\CCSet\Control\Class\<GUID>\Properties\Security.
    // An administrator can override the security descriptor by modifying
    // the registry.
    //
    RtlInitUnicodeString( &SddlString, L"D:P(A;;GA;;;SY)(A;;GA;;;BA)");

    //
    // Create a named deviceobject
    //

    RC = IoCreateDeviceSecure( g_DriverObject,
                               0x0,
                               &CommunicationObjectName,
                               FILE_DEVICE_UNKNOWN,
                               FILE_DEVICE_SECURE_OPEN,
                               FALSE, 
                               &SddlString,
                               (LPCGUID)&GUID_PCT_CONTROL_OBJECT,
                               &g_CommunicationDeviceObject);

    if( NT_SUCCESS( RC ) ){

        //
        // create a symbolic link
        //

        UNICODE_STRING      SymbolicLinkName;

        RtlInitUnicodeString( &SymbolicLinkName, PCT_DEVICE_SYMBOLIC_NAME );

        //
        // get ready to accept incoming requests,
        //

        //
        // use the direct IO so the system will
        // prepare MDL for us, i.e. the system will
        // call MmProbeAndLockPages on a user supplied
        // buffer
        //
        g_CommunicationDeviceObject->Flags |= DO_DIRECT_IO;

        RC = IoCreateSymbolicLink( &SymbolicLinkName, &CommunicationObjectName );
        if( NT_SUCCESS( RC ) ){

            g_CommunicationDeviceObject->Flags &= ~DO_DEVICE_INITIALIZING;

            //
            // from now the driver is ready to accept incoming requests
            //

        } else {

            DebugPrint( ( "IoCreateSymbolicLink failed %x\n", RC ) );
        }

    } else {

        DebugPrint( ("IoCreateDevice failed %x\n", RC ) );
    }

    ASSERT( NT_SUCCESS( RC ) );
    ASSERT( NULL != g_CommunicationDeviceObject );
    ASSERT( g_CommunicationDeviceObject?(0x0 == (g_CommunicationDeviceObject->Flags & DO_DEVICE_INITIALIZING) ): TRUE );

    if( !NT_SUCCESS( RC ) ){

        PctRemoveCommunicationDeviceObject();
    }

    return RC;
}

//-----------------------------------------------------------------

VOID
PctRemoveCommunicationDeviceObject()
/*

The function removes the object and the symbolic link created
by PctCreateCommunicationDeviceObject.

The function is idempotent if a caller guarantees that the
calls are mutually exclusive.

*/
{
    UNICODE_STRING    SymbolicLinkName;

    if( NULL == g_CommunicationDeviceObject )
        return;

    RtlInitUnicodeString( &SymbolicLinkName, PCT_DEVICE_SYMBOLIC_NAME );
    IoDeleteSymbolicLink( &SymbolicLinkName );

    if( NULL != g_CommunicationDeviceObject )
        IoDeleteDevice( g_CommunicationDeviceObject );

    g_CommunicationDeviceObject = NULL;
}

//-----------------------------------------------------------------

NTSTATUS
PctStartWorkerThreadsPool()
/*
The function initializes a worker thread pool used for
aynchronous requests processing

The counterpart function is PctStopWorkerThreadsPool
*/
{
    NTSTATUS  RC = STATUS_SUCCESS;
    ULONG     CpusNumber;
    ULONG     i;

    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );
    ASSERT( 0x0 == g_NumberOfWorkerThreads );

    //
    // it is impossible to wait on number of objects exceeding
    // MAXIMUM_WAIT_OBJECTS
    //
    ASSERT( MAX_NUMBER_OF_WORKER_THREADS <= MAXIMUM_WAIT_OBJECTS );

    KeInitializeEvent( &g_WorkerThreadsPoolEvent, SynchronizationEvent, FALSE );

    //
    // the stop event is a notification event so all waiting threads will become awake
    //
    KeInitializeEvent( &g_WorkerThreadsPoolStopEvent, NotificationEvent, FALSE );

    KeInitializeSpinLock( &g_WorkerThreadsPoolQueueLock );
    InitializeListHead( &g_WorkerThreadsPoolInputQueueHead );

    //
    // calculate the number of threads in the pool
    //
    CpusNumber = PstNumberOfProcessorUnits();
    g_NumberOfWorkerThreads = 2*CpusNumber;

    if( g_NumberOfWorkerThreads > MAX_NUMBER_OF_WORKER_THREADS ){

        //
        // hmmm, it seems it's a big machine
        //
        ASSERT( !"Check the PctStartWorkerThreadsPool correctness" );
        g_NumberOfWorkerThreads = MAX_NUMBER_OF_WORKER_THREADS;
    }

    //
    // start the threads
    //
    for( i = 0x0; i < g_NumberOfWorkerThreads; ++i ){

        HANDLE    ThreadHandle;

        //
        // start the thread
        //
        RC = PsCreateSystemThread( &ThreadHandle,
                                   (ACCESS_MASK)0L, 
                                   NULL, 
                                   NULL, 
                                   NULL, 
                                   PctWorkerThreadRoutine,
                                   NULL );
        if( !NT_SUCCESS( RC ) ){

            if( 0x0 == i )
                break;

            //
            // so we managed to start at least one thread then
            // consider this as a patial success
            //

            RC = STATUS_SUCCESS;
            g_NumberOfWorkerThreads = ( i+0x1 );// i is an array index!
            break;
        }

        RC = ObReferenceObjectByHandle( ThreadHandle,
                                        THREAD_ALL_ACCESS,
                                        NULL, 
                                        KernelMode, 
                                        &g_WorkerThreadsPool[ i ],
                                        NULL );

        if( !NT_SUCCESS( RC ) ){

            //
            // it is hard to restore from this point
            // as the code which stops worker threads waits 
            // on the objects, not handles, so kill all
            // threads and return errors
            //

            KeSetEvent( &g_WorkerThreadsPoolStopEvent,
                        IO_DISK_INCREMENT,
                        FALSE );

            //
            // at first wait for the last thread termination
            // which object was not obtained through the handle
            //
            ZwWaitForSingleObject( ThreadHandle, FALSE, NULL );

            //
            // adjust the number of valid entries
            //
            if( i != 0x0 )
                g_NumberOfWorkerThreads = i;// i is an array index!
            else
                g_NumberOfWorkerThreads = 0x0;

            //
            // stop all other threads
            //
            PctStopWorkerThreadsPool();

        }// if

        //
        // close the handle as the object pointer 
        // has been obtained and referenced
        //
        ZwClose( ThreadHandle );

        if( !NT_SUCCESS( RC ) )
            break;

    }// for 

    return RC;
}

//-------------------------------------------------

VOID
PctStopWorkerThreadsPool()
//
// the function notifies all worker threads that
// they must stop and WAIT untill all threads have
// stopped
//
// The function is idempotent if the caller
// guarantees the mutual exclusion
//
{

    //
    // waiting on APC_LEVEL is allowed but
    // looks strange for this function ( called from APC
    // or with mutex held? Why?)
    //
    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );

    if( 0x0 == g_NumberOfWorkerThreads ){

        //
        // nothing to do or pool has not been initialized
        //
        return;
    }

    //
    // notify all threads that thy must stop
    //
    KeSetEvent( &g_WorkerThreadsPoolStopEvent,
                IO_DISK_INCREMENT,
                FALSE );

    DebugPrint( ("Stopping the pool threads ...") );

    //
    // it is a good idea to specify timeout and wait in a cicle
    // putting a message if the timeout has expired
    //
    KeWaitForMultipleObjects( g_NumberOfWorkerThreads,
                              (PVOID)g_WorkerThreadsPool,
                              WaitAll,
                              Executive,
                              KernelMode,
                              FALSE,
                              NULL,
                              g_WorkerThreadsPoolWaitBlock );

    DebugPrint( ("The pool threads stopped") );

    g_NumberOfWorkerThreads = 0x0;
}

//-------------------------------------------------

VOID
PctWorkerThreadRoutine(
    __in_opt PVOID    Context
    )
{
    //
    // remember about THREAD_WAIT_OBJECTS !
    //
    PKEVENT    Events[ 0x2 ];

    ASSERT( KeGetCurrentIrql() == PASSIVE_LEVEL );
    ASSERT( sizeof( Events )/sizeof( Events[ 0x0 ] ) <= THREAD_WAIT_OBJECTS );

    Events[ 0x0 ] = &g_WorkerThreadsPoolEvent;
    Events[ 0x1 ] = &g_WorkerThreadsPoolStopEvent;

    while ( TRUE ){

        NTSTATUS       RC;
        PLIST_ENTRY    request;

        //
        // wait for the wake up events
        //
        RC = KeWaitForMultipleObjects( sizeof( Events )/sizeof( Events[0x0] ),
                                       Events,
                                       WaitAny,
                                       Executive,
                                       KernelMode,
                                       FALSE,
                                       NULL,
                                       NULL );

        //
        // process the list of deferred jobs
        //
        while( request = ExInterlockedRemoveHeadList( &g_WorkerThreadsPoolInputQueueHead, &g_WorkerThreadsPoolQueueLock ) ){

            NTSTATUS             IoRC;
            PIRP                 UserIrp;
            BOOLEAN              PendingReturned;
            PPCT_USER_REQUEST    UserRequest = CONTAINING_RECORD( request, PCT_USER_REQUEST, ListEntry );

            UserIrp = UserRequest->Irp;
            PendingReturned = ( 0x1 == UserRequest->PendingReturned );

#if DBG
            InitializeListHead( &UserRequest->ListEntry );
#endif//DBG
            //
            // check the queue without acquiring the lock as this is a benign operation
            //
            if( !IsListEmpty( &g_WorkerThreadsPoolInputQueueHead ) ){

                //
                // try to wake up another thread
                // with a priority not exceeding the current thread's one
                //
                KeSetEvent( &g_WorkerThreadsPoolEvent,
                            IO_NO_INCREMENT,
                            FALSE );

            }// if

            //
            // issue a request to a disk
            //
            IoRC = PctProcessUserReadWriteRequest( UserRequest );
            PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( UserRequest );
            ASSERT( STATUS_PENDING != IoRC );

            //
            // complete the user IRP
            //
            UserIrp->IoStatus.Status = RC;

            //
            // if the request is asynchronous the output buffer is NULL
            //
            if( NT_SUCCESS( RC ) && !PendingReturned )
                UserIrp->IoStatus.Information = sizeof( PCT_IO_REQUEST );
            else
                UserIrp->IoStatus.Information = 0x0;

            IoCompleteRequest( UserIrp, IO_DISK_INCREMENT );

        }// while

        if( STATUS_WAIT_1 == RC ){

            //
            // the thread must be terminated
            //
            PsTerminateSystemThread( STATUS_SUCCESS );

        }

    }// while

#if DBG
    //
    // unattainable code
    //
    KeBugCheckEx( 0x777777,
                  (ULONG_PTR)__LINE__,
                  (ULONG_PTR)Context,
                  (ULONG_PTR)PsGetCurrentThread(),
                  (ULONG_PTR)PsGetCurrentProcess() );
#endif//DBG
}

//-------------------------------------------------

ULONG
PstNumberOfProcessorUnits()
/*
the function returns the number of active CPUs,
i.e. the "Number of Physical CPUs" multiplied number of cores
for each physical CPUs
*/
{
    ULONG    NumberOfProcessorUnits = 0x0;

    //
    // Zw* function can be called only at PASSIVE_LEVEL
    //
    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );

#ifdef _AMD64_
    {
        KAFFINITY   ProcessorsMask;
        ULONG       i;
        ProcessorsMask = KeQueryActiveProcessors();
        i = 0x0;
        while( i < ( sizeof( ProcessorsMask )*0x8 ) ){

            if( 0x0 != ( ProcessorsMask & (((ULONG_PTR)0x1)<<i) ) )
                ++NumberOfProcessorUnits;

            ++i;
        }
    }
#else//_AMD64_
    {
        NTSTATUS    RC;
        SYSTEM_BASIC_INFORMATION    SystemInformation;

        //
        // get the system information
        //
        RC = ZwQuerySystemInformation( SystemBasicInformation,
                                       &SystemInformation,
                                       sizeof( SystemInformation ),
                                       NULL );

        if( NT_SUCCESS( RC ) )
            NumberOfProcessorUnits = SystemInformation.NumberProcessors;

        ASSERT( NT_SUCCESS( RC ) );
    }
#endif//else _AMD64_

    ASSERT( 0x0 != NumberOfProcessorUnits );

    //
    // in case of error set the number of CPUs to two - 
    // two cores CPUs are experiencing the great proliferation
    //
    if( 0x0 == NumberOfProcessorUnits )
        NumberOfProcessorUnits = 0x2;

    return NumberOfProcessorUnits;
}

//-----------------------------------------------------------------

NTSTATUS
PctProcessUserReadWriteRequest(
    __in PPCT_USER_REQUEST UserRequest
    )
/*
The function processes the user request and frees UserRequest
allocated by the caller from the NonPagedPool, the caller must
not use UserRequest after calling this function
*/
{
    NTSTATUS                RC;
    PIRP                    Irp = NULL;
    PIO_STACK_LOCATION      UserIrpStackLocation;
    PIO_STACK_LOCATION      DiskIrpStackLocation;
    PDEVICE_OBJECT          DiskDeviceObject;
    PPCT_FILE_FS_CONTEXT    FsContext;
    IO_STATUS_BLOCK         IoStatusBlock;
    KEVENT                  Event;

    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );
    ASSERT( NULL != UserRequest->OriginalUserRequest );

    KeInitializeEvent( &Event, SynchronizationEvent, FALSE );

    UserIrpStackLocation = IoGetCurrentIrpStackLocation( UserRequest->Irp );

    FsContext = UserIrpStackLocation->FileObject->FsContext;
    ASSERT( NULL != FsContext );

    //
    // get the current upper device pointer, so if the filter attached
    // after the PnP manager built the stack this filter will see our request,
    // The device should not be detached until the remove request is received.
    //
    DiskDeviceObject = IoGetAttachedDevice( FsContext->DiskFileObject->DeviceObject );

    //
    // map the Mdl in the system space, the Mdl will be unmapped
    // by the system when the user's IRP will be completed
    //
    UserRequest->BufferSystemAddress = MmGetSystemAddressForMdlSafe( UserRequest->UserBufferMdl,
        NormalPagePriority );

    if( NULL != UserRequest->BufferSystemAddress ){

        //
        // Allocate an IRP, there is a point for optimization here if the disk uses DIRECT_IO
        //   - allocate an Irp by calling IoAllocateIrp
        //   - use the already allocated MDL
        // the current solution allocates a new MDL, so the overhead is the memory
        // for MDL structure and this MDL is mapped by the disk driver( while 
        // UserRequest->UserBufferMdl  has been already mapped ), so it is a good idea
        // to reduce the pressure on the system PTEs by adopting the solution mentioned above
        //
        Irp = IoBuildSynchronousFsdRequest( ( 0x1 == UserRequest->ReadOperation )? IRP_MJ_READ : IRP_MJ_WRITE,
                                            DiskDeviceObject,
                                            UserRequest->BufferSystemAddress,
                                            UserRequest->Size,
                                            &UserRequest->Offset,
                                            &Event,
                                            &IoStatusBlock );

    }

    if( NULL == Irp ) {

        RC = STATUS_INSUFFICIENT_RESOURCES;

        //
        // set the suer's Irp status and information
        //
        UserRequest->Irp->IoStatus.Status = RC;
        UserRequest->Irp->IoStatus.Information = 0x0;

        //
        // complete the user's Irp
        //
        IoCompleteRequest( UserRequest->Irp, IO_DISK_INCREMENT );

        //
        // unmap and unlock
        //
        MmUnlockPages( UserRequest->UserBufferMdl );

        //
        // free
        //
        IoFreeMdl( UserRequest->UserBufferMdl );

        //
        // free the memory allocated by the caller
        //
        ExFreePoolWithTag( UserRequest, PCT_MEMORY_TAG );
        PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( UserRequest );

        return RC;
    }

    //
    // send the IRP to the disk stack
    //

    RC = IoCallDriver( DiskDeviceObject, Irp );

    //
    // Wait for the IRP completion, so there is another 
    // opportunity for optimization as the waiting in
    // the worker thread reduces the driver througput,
    // so if you reach the limit either increase the number
    // of worker threads or get rid of this waiting, in the 
    // latter case do not forget to not use variables allocated 
    // on the stack in the IRP
    //
    if( STATUS_PENDING == RC ){

        KeWaitForSingleObject( &Event,
                               Executive,
                               KernelMode,
                               FALSE,
                               NULL );

        RC = IoStatusBlock.Status;
    }

    UserRequest->OriginalUserRequest->BytesReadOrWritten = (ULONG)IoStatusBlock.Information;

    //
    // unmap and unlock
    //
    MmUnlockPages( UserRequest->UserBufferMdl );

    //
    // free
    //
    IoFreeMdl( UserRequest->UserBufferMdl );

    ExFreePoolWithTag( UserRequest, PCT_MEMORY_TAG );
    PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( UserRequest );

    return RC;
}

//-----------------------------------------------------------------

NTSTATUS
PctProcessUserIoRequest(
    __in PIRP              UserIrp,
    __in PPCT_IO_REQUEST   Request
    )
{
    NTSTATUS            RC = STATUS_SUCCESS;
    PPCT_USER_REQUEST   UserRequest = NULL;
    BOOLEAN             FreeTheUserRequest = TRUE;
    PFILE_OBJECT        FileObject;
    PPCT_FILE_FS_CONTEXT    FsContext;

    ASSERT( PASSIVE_LEVEL == KeGetCurrentIrql() );

    FileObject = IoGetCurrentIrpStackLocation( UserIrp )->FileObject;
    FsContext = FileObject->FsContext;
    ASSERT( NULL != FsContext );

    //
    // check the user supplied buffer
    //
    __try{

        if( 0x1 == Request->ReadOperation ){

            ProbeForWrite( Request->UserBuffer,
                           Request->BufferSize,
                           1 );

        } else {

            ASSERT( 0x1 == Request->WriteOperation );

            ProbeForRead( Request->UserBuffer,
                          Request->BufferSize,
                          1 );

        }

    } __except( EXCEPTION_EXECUTE_HANDLER ) {

        RC = GetExceptionCode();
        return RC;
    }

    UserRequest = ExAllocatePoolWithTag( NonPagedPool,
                                         sizeof( *UserRequest ),
                                         PCT_MEMORY_TAG );
    if( NULL == UserRequest ){
        return STATUS_INSUFFICIENT_RESOURCES;
    }

    RtlZeroMemory( UserRequest, sizeof( *UserRequest ) );

#if DBG
    InitializeListHead( &UserRequest->ListEntry );
#endif//DBG

    UserRequest->Irp = UserIrp;
    UserRequest->ReadOperation = Request->ReadOperation;
    UserRequest->WriteOperation = Request->WriteOperation;
    UserRequest->OriginalUserRequest = Request;

    UserRequest->Offset.QuadPart = (Request->OffsetInSectors.QuadPart)*FsContext->DiskGeometry.BytesPerSector;
    UserRequest->Size = Request->NumberOfSectors*FsContext->DiskGeometry.BytesPerSector;

    //
    // check that the buffer is aligned to a sector size
    // if you want to process unaligned buffers you have to
    // use the intermediate buffer
    //
    if( 0x0 != ((ULONG_PTR)Request->UserBuffer)%FsContext->DiskGeometry.BytesPerSector ){

        RC = STATUS_INVALID_PARAMETER;
        goto __exit;
    }

    //
    // check that the buffer is big enougth
    //
    if( UserRequest->Size > Request->BufferSize ){

        RC = STATUS_BUFFER_TOO_SMALL;
        goto __exit;
    }

    //
    // lock the user buffer, the lock is made here as I suppose that 
    // the application will prefer to use the asynchronous IO for the
    // majority of the requests.
    // Why is the asynchronous IO so important in this case?
    // Because the disk driver uses an IO scheduler and asynchronous
    // requests might significantly improve the application throughput.
    //
    UserRequest->UserBufferMdl = IoAllocateMdl( Request->UserBuffer,
                                                Request->BufferSize,
                                                FALSE,
                                                FALSE,
                                                NULL );

    if( NULL == UserRequest->UserBufferMdl ){

        RC = STATUS_INSUFFICIENT_RESOURCES;
        goto __exit;
    }

    __try {

        MmProbeAndLockPages( UserRequest->UserBufferMdl,
                             ExGetPreviousMode(),
                             (LOCK_OPERATION) ( (0x1 == Request->ReadOperation) ? IoWriteAccess : IoReadAccess) );

    } __except( EXCEPTION_EXECUTE_HANDLER ) {

        IoFreeMdl( UserRequest->UserBufferMdl );
        UserRequest->UserBufferMdl= NULL;

        RC = GetExceptionCode();
        goto __exit;
    }

    //
    // PctProcessUserReadWriteRequest will free UserRequest
    // and complete UserIrp if the request is asynchronous
    // else the Irp must be completed by the caller
    //
    FreeTheUserRequest = FALSE;

    if( IoIsOperationSynchronous( UserIrp ) ){

        //
        // this is a synchronous operation
        //
        PctProcessUserReadWriteRequest( UserRequest );
        PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( UserRequest );

    } else {

        //
        // prepare the irp for pending as the user wants
        // the request to be processed asynchronously
        //
        IoMarkIrpPending( UserIrp );
        RC = STATUS_PENDING;

        UserRequest->PendingReturned = 0x1;

        //
        // post the request in a worker thread
        //
        ExInterlockedInsertTailList( &g_WorkerThreadsPoolInputQueueHead,
                                     &UserRequest->ListEntry,
                                     &g_WorkerThreadsPoolQueueLock );
        PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( UserRequest );

        //
        // wakeup a waiting thread
        //
        KeSetEvent( &g_WorkerThreadsPoolEvent,
                    IO_DISK_INCREMENT,
                    FALSE );

    }

__exit:

    if( FreeTheUserRequest && NULL != UserRequest ){

        ASSERT( PCT_IS_POINTER_VALID( UserRequest ) );

        if( UserRequest->UserBufferMdl ){

            //
            // unmap and unlock
            //
            MmUnlockPages( UserRequest->UserBufferMdl );

            //
            // free
            //
            IoFreeMdl( UserRequest->UserBufferMdl );
        }

        ExFreePoolWithTag( UserRequest, PCT_MEMORY_TAG );
        PCT_DEBUG_SET_POINTER_TO_INVALID_VALUE( UserRequest );
    }

    return RC;
}

//-----------------------------------------------------------------

NTSTATUS
PctCreateDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    )
{
    NTSTATUS                RC = STATUS_SUCCESS;
    PFILE_OBJECT            FileObject;
    IO_STATUS_BLOCK         IoStatus;
    HANDLE                  DiskHandle;
    PPCT_FILE_FS_CONTEXT    FsContext;
    OBJECT_ATTRIBUTES       ObjectAttributes;

    FileObject = IoGetCurrentIrpStackLocation( Irp )->FileObject;

    //
    // initialize proprietary FsContext
    //
    FileObject->FsContext = ExAllocatePoolWithTag( NonPagedPool,
                                                   sizeof( PCT_FILE_FS_CONTEXT ),
                                                   PCT_MEMORY_TAG );
    if( NULL == FileObject->FsContext ){

        RC = STATUS_INSUFFICIENT_RESOURCES;
        Irp->IoStatus.Status = RC;
        Irp->IoStatus.Information = 0x0;

        IoCompleteRequest( Irp, IO_NO_INCREMENT );

        return RC;
    }

    FsContext = (PPCT_FILE_FS_CONTEXT)FileObject->FsContext;

    RtlZeroMemory( FileObject->FsContext, sizeof( PCT_FILE_FS_CONTEXT ) );

    InitializeObjectAttributes( &ObjectAttributes,
                                &FileObject->FileName,
                                OBJ_CASE_INSENSITIVE | OBJ_KERNEL_HANDLE,
                                NULL,
                                NULL
                                );

    //
    // issue a create request on a disk stack
    //
    RC = ZwCreateFile( &DiskHandle,
                       GENERIC_READ | GENERIC_WRITE,
                       &ObjectAttributes,
                       &IoStatus,
                       NULL, 
                       FILE_ATTRIBUTE_NORMAL,
                       FILE_SHARE_READ | FILE_SHARE_WRITE,
                       FILE_OPEN,
                       FILE_SYNCHRONOUS_IO_NONALERT,//FILE_NON_DIRECTORY_FILE,
                       NULL,
                       0
                       );

    if( !NT_SUCCESS( RC ) ){

        ExFreePoolWithTag( FileObject->FsContext, PCT_MEMORY_TAG );

        FileObject->FsContext = NULL;

        Irp->IoStatus.Status = RC;
        Irp->IoStatus.Information = 0x0;

        IoCompleteRequest( Irp, IO_NO_INCREMENT );

        return RC;
    }

    RC = ObReferenceObjectByHandle( DiskHandle,
                                    FILE_ANY_ACCESS,
                                    *IoFileObjectType,
                                    KernelMode, //to avoid a security check set mode to Kernel
                                    (PVOID*)&FsContext->DiskFileObject,
                                    NULL );
    if( !NT_SUCCESS( RC ) ){

        ExFreePoolWithTag( FileObject->FsContext, PCT_MEMORY_TAG );

        FileObject->FsContext = NULL;

        Irp->IoStatus.Status = RC;
        Irp->IoStatus.Information = 0x0;

        IoCompleteRequest( Irp, IO_NO_INCREMENT );

        ZwClose( DiskHandle );
        return RC;
    }

    //
    // get the drive geometry
    //
    RC = PctGetDriveGeometry( IoGetAttachedDevice( FsContext->DiskFileObject->DeviceObject ),
                              &FsContext->DiskGeometry );
    if( !NT_SUCCESS( RC ) ){

        ExFreePoolWithTag( FileObject->FsContext, PCT_MEMORY_TAG );

        FileObject->FsContext = NULL;

        Irp->IoStatus.Status = RC;
        Irp->IoStatus.Information = 0x0;

        IoCompleteRequest( Irp, IO_NO_INCREMENT );

        ZwClose( DiskHandle );
        return RC;
    }

    ASSERT( NT_SUCCESS( RC ) );

    ZwClose( DiskHandle );

    Irp->IoStatus.Status = RC;
    Irp->IoStatus.Information = 0x0;

    IoCompleteRequest( Irp, IO_NO_INCREMENT );

    return RC;
}

//-----------------------------------------------------------------

NTSTATUS
PctCloseDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    )
{
    NTSTATUS                RC = STATUS_SUCCESS;
    PFILE_OBJECT            FileObject;
    PPCT_FILE_FS_CONTEXT    FsContext;

    FileObject = IoGetCurrentIrpStackLocation( Irp )->FileObject;
    FsContext = (PPCT_FILE_FS_CONTEXT)FileObject->FsContext;

    //
    // dereference the file object referenced in PctCreateDispatch
    //
    ObDereferenceObject( FsContext->DiskFileObject );

    //
    // fre the memory allocated in PctCreateDispatch
    //
    ExFreePoolWithTag( FileObject->FsContext, PCT_MEMORY_TAG );

    FileObject->FsContext = NULL;

    Irp->IoStatus.Status = RC;
    Irp->IoStatus.Information = 0x0;

    IoCompleteRequest( Irp, IO_NO_INCREMENT );

    return RC;
}

//-----------------------------------------------------------------

NTSTATUS
PctCleanupDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    )
{
    NTSTATUS      RC = STATUS_SUCCESS;

    Irp->IoStatus.Status = RC;
    Irp->IoStatus.Information = 0x0;

    IoCompleteRequest( Irp, IO_NO_INCREMENT );

    return RC;
}

//-----------------------------------------------------------------

NTSTATUS
PctDeviceIoctlDispatch(
    __in PDEVICE_OBJECT    DeviceObject,
    __in PIRP    Irp
    )
{
    NTSTATUS           RC = STATUS_SUCCESS;
    PPCT_IO_REQUEST    Request;
    ULONG              OutputBufferLength;
    ULONG              InputBufferLength;

    Request = (PPCT_IO_REQUEST)Irp->AssociatedIrp.SystemBuffer;

    InputBufferLength = IoGetCurrentIrpStackLocation(Irp)->Parameters.DeviceIoControl.InputBufferLength;
    OutputBufferLength = IoGetCurrentIrpStackLocation(Irp)->Parameters.DeviceIoControl.OutputBufferLength;

    //
    // set the 0x0 in advance
    //
    Irp->IoStatus.Information = 0x0;

    //
    // if the user wants to use asyncronous IO the output buffer should be NULL
    //
    if( InputBufferLength < sizeof( *Request ) || 
        ( OutputBufferLength != 0x0 && 
          OutputBufferLength < sizeof( *Request ) ) ){

        RC = STATUS_BUFFER_TOO_SMALL;
        goto __exit;
    }

    if( Request->WriteOperation == Request->ReadOperation ){

        RC = STATUS_INVALID_PARAMETER;
        goto __exit;
    }

    switch( IoGetCurrentIrpStackLocation(Irp)->Parameters.DeviceIoControl.IoControlCode ){

        case PCT_IOCTL_IO_REQUEST:

            //
            // if PctProcessUserIoRequest returns 
            // STATUS_PENDING the Irp must not be completed here
            // as the Irp has been sent in a worker thread
            //
            RC = PctProcessUserIoRequest( Irp, Request );
            break;

        default:
            RC = STATUS_INVALID_DEVICE_REQUEST;
            break;
    }

__exit:

    if( RC != STATUS_PENDING ){

        //
        // request has not been made pending
        //

        if( NT_SUCCESS( RC ) && 0x0 != OutputBufferLength )
            Irp->IoStatus.Information = sizeof( *Request );
        else
            Irp->IoStatus.Information = 0x0;

        Irp->IoStatus.Status = RC;
        IoCompleteRequest( Irp, IO_NO_INCREMENT );
    }

    return RC;
}

//-----------------------------------------------------------------

NTSTATUS
PctDrvGeometrySignalCompletion(
    IN PDEVICE_OBJECT DeviceObject,
    IN PIRP Irp,
    IN PKEVENT Event
    )
{

    //
    // I an an Irp creator so there is no need to
    // propagate the pending flag, moreover there is
    // no stack location for this
    //

    KeSetEvent(Event, IO_NO_INCREMENT, FALSE);

    return STATUS_MORE_PROCESSING_REQUIRED;
}

//-----------------------------------------------------------------

NTSTATUS
PctGetDriveGeometry( 
    __in PDEVICE_OBJECT    DiskDeviceObject, 
    __out DISK_GEOMETRY*   PtrDiskGeometry 
    )
/*++
--*/
{
    PIRP                 Irp;
    PIO_STACK_LOCATION   IrpStack;
    KEVENT               Event;
    NTSTATUS             RC;

    //
    // Build an Irp to send IOCTL_DISK_GET_DRIVE_GEOMETRY
    //

    Irp = IoAllocateIrp( DiskDeviceObject->StackSize,
                         FALSE );

    if( Irp == NULL ){
        return STATUS_INSUFFICIENT_RESOURCES;
    }

    IrpStack = IoGetNextIrpStackLocation( Irp );

    IrpStack->MajorFunction = IRP_MJ_DEVICE_CONTROL;

    IrpStack->Parameters.DeviceIoControl.IoControlCode = IOCTL_DISK_GET_DRIVE_GEOMETRY;
    IrpStack->Parameters.DeviceIoControl.OutputBufferLength = sizeof( DISK_GEOMETRY );

    Irp->AssociatedIrp.SystemBuffer = PtrDiskGeometry;

    KeInitializeEvent( &Event, SynchronizationEvent, FALSE );

    IoSetCompletionRoutine( Irp,
                            PctDrvGeometrySignalCompletion,
                            &Event,
                            TRUE,
                            TRUE,
                            TRUE );

    RC = IoCallDriver( DiskDeviceObject,
                       Irp );

    KeWaitForSingleObject( &Event,
                           Executive,
                           KernelMode,
                           FALSE,
                           NULL );

    RC = Irp->IoStatus.Status;

    IoFreeIrp( Irp );

    return RC;
}

//-----------------------------------------------------------------
