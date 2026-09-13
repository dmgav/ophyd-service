import pytest

from ophyd_service.manager.worker_utils import _device_name_matches_pattern, device_name_matches_patterns


# fmt: off
@pytest.mark.parametrize(
    "device_name, pattern, result",
    [
        # ------------------------------------------------------------
        # Explicitly stated device and subdevice names
        ("det1", "det1", True),
        ("det1", "det2", False),
        ("det1", "det", False),  # The name must match exactly
        ("det1", "det1.val", False),
        ("det1.val", "det1.val", True),
        ("det1.val", "det1", False),
        ("det1.val", "det1.val2", False),
        ("sim_stage.det.val", "sim_stage.det.val", True),
        ("sim_stage.det.val", "sim_stage.det", False),

        # ------------------------------------------------------------
        # Single regular expression. ':' is not part of the expression: it indicates
        # that the string is a pattern.
        ("det", ":det", True),
        ("mydetector", ":det", True),  # Not anchored, matches any part of the name
        ("abc", ":det", False),
        ("det", ":^det$", True),
        ("mydetector", ":^det$", False),
        ("det1.val", ":^det", False),  # The pattern has one component, the name has two
        ("det1", ":^det:^val$", True),  # The name may be shorter than the pattern
        ("det1.val", ":^det:^val$", True),
        ("det1.val2", ":^det:^val$", False),
        ("det1.val.x", ":^det:^val$", False),  # The name is longer than the pattern

        # ------------------------------------------------------------
        # Multiple expressions: each expression is applied to the respective
        # component of the name. All matching names are selected by default.
        ("sim_stage_A", ":^sim:^mt:^x$", True),
        ("sim_stage_A.mtrs", ":^sim:^mt:^x$", True),
        ("sim_stage_A.mtrs.x", ":^sim:^mt:^x$", True),
        ("sim_stage_B.mtrs.x", ":^sim:^mt:^x$", True),
        ("sim_stage_A.mtrs.y", ":^sim:^mt:^x$", False),
        ("stage_A.mtrs.x", ":^sim:^mt:^x$", False),
        ("sim_stage_A.det.x", ":^sim:^mt:^x$", False),
        ("sim_stage_A.mtrs.x.fld", ":^sim:^mt:^x$", False),

        # ------------------------------------------------------------
        # '+' explicitly selects the names ending at the given level (default behavior)
        ("sim_stage_A", ":+^sim:+^mt:+^x$", True),
        ("sim_stage_A.mtrs", ":+^sim:+^mt:+^x$", True),
        ("sim_stage_A.mtrs.x", ":+^sim:+^mt:+^x$", True),
        ("det.val", ":^det:+^val$", True),
        ("det.val", ":+^det:^val$", True),

        # ------------------------------------------------------------
        # '-' skips the names ending at the given level, the search continues deeper
        ("sim_stage_A", ":-^sim:^mt:^x$", False),
        ("sim_stage_A.mtrs", ":-^sim:^mt:^x$", True),
        ("sim_stage_A.mtrs.x", ":-^sim:^mt:^x$", True),
        ("sim_stage_A", ":-^sim:-^mt:-^x$", False),
        ("sim_stage_A.mtrs", ":-^sim:-^mt:-^x$", False),
        ("sim_stage_A.mtrs.x", ":-^sim:-^mt:-^x$", True),  # '-' is ignored in the last component
        ("det.val", ":^det:-^val$", True),  # Identical to ':^det:^val$'
        ("det", ":^det:-^val$", True),

        # ------------------------------------------------------------
        # '?' marks the expression applied to the remaining part of the name
        ("simval", ":?^sim.*val$", True),
        ("sim_stage_A.val", ":?^sim.*val$", True),
        ("sim_stage_A.det1.val", ":?^sim.*val$", True),
        ("sim_stage_A.detectors.det1.val", ":?^sim.*val$", True),
        ("sim_stage_A.det1.x", ":?^sim.*val$", False),
        ("stage_A.val", ":?^sim.*val$", False),

        # The full name expression may be preceded by the expressions for name components
        ("sim_stage_A", ":^sim_stage_A$:?.*val$", True),  # Selected by the first expression
        ("sim_stage_A.val", ":^sim_stage_A$:?.*val$", True),
        ("sim_stage_A.det1_val", ":^sim_stage_A$:?.*val$", True),
        ("sim_stage_A.det1.val", ":^sim_stage_A$:?.*val$", True),
        ("sim_stage_A.detectors.det1.val", ":^sim_stage_A$:?.*val$", True),
        ("sim_stage_A.det1.x", ":^sim_stage_A$:?.*val$", False),
        ("sim_stage_B.val", ":^sim_stage_A$:?.*val$", False),
        ("sim_stage_A", ":-^sim_stage_A$:?.*val$", False),  # The first level is skipped
        ("sim_stage_A.val", ":-^sim_stage_A$:?.*val$", True),

        # ------------------------------------------------------------
        # 'depth=N' limits the depth of the search for the full name expression
        ("sim_stage_A", ":+^sim_stage_A$:?.*val$:depth=2", True),
        ("sim_stage_A.val", ":+^sim_stage_A$:?.*val$:depth=2", True),
        ("sim_stage_A.det1_val", ":+^sim_stage_A$:?.*val$:depth=2", True),
        ("sim_stage_A.det1.val", ":+^sim_stage_A$:?.*val$:depth=2", True),
        ("sim_stage_A.detectors.det1.val", ":+^sim_stage_A$:?.*val$:depth=2", False),
        ("det_val", ":?.*val$:depth=1", True),
        ("det.val", ":?.*val$:depth=1", False),
        ("det.val", ":?.*val$:depth=2", True),
        ("det.sub.val", ":?.*val$:depth=2", False),
        ("det.sub.val", ":?.*val$:depth=3", True),

        # ------------------------------------------------------------
        # Device type keywords are accepted, but ignored: the type of the device
        # can not be determined from the name.
        ("sim_stage_A.x", "__MOTOR__:^sim_stage_A$:?.*", True),
        ("sim_stage_A.x", "__DETECTOR__:^sim_stage_A$:?.*", True),
        ("sim_stage_A.x", "__READABLE__:^sim_stage_A$:?.*", True),
        ("sim_stage_A.x", "__FLYABLE__:^sim_stage_A$:?.*", True),
        ("det2.x", "__MOTOR__:^sim_stage_A$:?.*", False),
        # Without ':' the keyword is a valid device name, not a type keyword
        ("__MOTOR__", "__MOTOR__", True),
        ("det1", "__MOTOR__", False),

        # ------------------------------------------------------------
        # Spaces are removed from the patterns before processing
        ("sim_stage_A.mtrs", " : ^sim : ^mt ", True),
        ("det1.val", "det1 . val", True),
    ],
)
# fmt: on
def test_device_name_matches_pattern(device_name, pattern, result):
    assert _device_name_matches_pattern(device_name, pattern) == result


# fmt: off
@pytest.mark.parametrize(
    "pattern, except_type, msg",
    [
        (":^det[", ValueError, "invalid regular expression"),
        ("", ValueError, "empty string"),
        (":", ValueError, "empty components"),
        (":^det:", ValueError, "empty components"),
        ("det1..val", ValueError, "is an empty string"),
        (".det1", ValueError, "invalid characters"),
        ("^det1$", ValueError, "invalid characters"),  # A pattern must be labeled with ':'
        ("__UNKNOWN__:^det$", ValueError, "is not supported"),
        (":?^det:^val$", ValueError, "followed by the depth specification"),
        (":?^det:^val$:depth=2", ValueError, "must be the last"),
        (":?^det$:size=2", ValueError, "followed by the depth specification"),
        (":?^det$:depth=0", ValueError, "must be positive integer"),
        (":?^det$:depth=a", ValueError, "incorrect format"),
        (10, TypeError, "incorrect type"),
        (None, TypeError, "incorrect type"),
    ],
)
# fmt: on
def test_device_name_matches_pattern_failing(pattern, except_type, msg):
    with pytest.raises(except_type, match=msg):
        _device_name_matches_pattern("det1.val", pattern)


# fmt: off
@pytest.mark.parametrize(
    "device_name, patterns, result",
    [
        ("det1", [], False),  # Empty list of patterns matches nothing
        ("det1", ["det1"], True),
        ("det1", ["det2"], False),
        ("det1", ["det2", "det3", "det1"], True),  # Matches one of the patterns
        ("det1", ["det2", "det3"], False),
        ("det1", ["det2", ":^det1$"], True),  # Names and patterns may be mixed
        ("sim_stage_A.mtrs.x", [":^det", ":-^sim:-^mt:-^x$"], True),
        ("sim_stage_A.mtrs", [":^det", ":-^sim:-^mt:-^x$"], False),
    ],
)
# fmt: on
def test_device_name_matches_patterns(device_name, patterns, result):
    assert device_name_matches_patterns(device_name, patterns) == result


def test_device_name_matches_patterns_failing():
    # The exception is raised if any of the patterns is invalid
    with pytest.raises(ValueError, match="invalid regular expression"):
        device_name_matches_patterns("det1", ["det2", ":^det["])
