package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.AllSelector;
import io.mubel.api.grpc.v1.events.EventSelector;
import io.mubel.api.grpc.v1.events.StreamSelector;

public class EventSelectorValidator {

    private static final Validator<AllSelector> ALL_SELECTOR_VALIDATOR = ValidatorBuilder.<AllSelector>of()
            .constraint(AllSelector::getFromSequenceNo, "fromSequenceNo", seq -> seq.greaterThanOrEqual(0L))
            .build();

    private static final Validator<StreamSelector> STREAM_SELECTOR_VALIDATOR = ValidatorBuilder.<StreamSelector>of()
            .constraint(StreamSelector::getStreamId, "streamId", CommonConstraints.streamId())
            .constraint(StreamSelector::getFromRevision, "fromRevision", rev -> rev.greaterThanOrEqual(0))
            .constraint(StreamSelector::getToRevision, "toRevision", rev -> rev.greaterThanOrEqual(0))
            .constraintOnTarget(selector -> {
                if (selector.hasFromRevision() && selector.hasToRevision()) {
                    return selector.getToRevision() > selector.getFromRevision();
                } else {
                    return true;
                }
            }, "toRevision", "toRevision.isGreaterThanFrom", "\"toRevision\" must be greater than \"fromRevision\"")
            .build();

    public static Validator<EventSelector> EVENT_SELECTOR_VALIDATOR = ValidatorBuilder.<EventSelector>of()
            .constraintOnCondition((op, group) -> op.hasStream(), op -> op.nest(EventSelector::getStream, "stream", STREAM_SELECTOR_VALIDATOR))
            .constraintOnCondition((op, group) -> op.hasAll(), op -> op.nest(EventSelector::getAll, "all", ALL_SELECTOR_VALIDATOR))
            .build();

}
