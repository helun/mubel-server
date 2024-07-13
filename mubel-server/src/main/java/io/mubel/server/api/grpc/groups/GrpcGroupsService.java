package io.mubel.server.api.grpc.groups;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.v1.groups.*;
import io.mubel.server.spi.groups.GroupManager;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.support.IdGenerator;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class GrpcGroupsService extends GroupsServiceGrpc.GroupsServiceImplBase {

    private final GroupManager groupManager;
    private final IdGenerator idGenerator;

    public GrpcGroupsService(GroupManager groupManager, IdGenerator idGenerator) {
        this.groupManager = groupManager;
        this.idGenerator = idGenerator;
    }

    @Override
    public void join(JoinGroupRequest request, StreamObserver<GroupStatus> responseObserver) {
        var joinRequest = new JoinRequest(
                request.getEsid(),
                request.getGroupId(),
                idGenerator.generateStringId()
        );
        groupManager.join(joinRequest)
                .subscribe(
                        responseObserver::onNext,
                        responseObserver::onError,
                        responseObserver::onCompleted
                );
    }

    @Override
    public void leaveConsumerGroup(LeaveGroupRequest request, StreamObserver<Empty> responseObserver) {
        super.leaveConsumerGroup(request, responseObserver);
    }

    @Override
    public void heartbeat(Heartbeat request, StreamObserver<Empty> responseObserver) {
        super.heartbeat(request, responseObserver);
    }
}
