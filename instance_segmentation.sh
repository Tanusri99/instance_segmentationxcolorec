#!/bin/bash
set -e

function init_variables() {
    print_help_if_needed $@
    script_dir=$(dirname $(realpath "$0"))
    source $script_dir/../../../../../scripts/misc/checks_before_run.sh

    readonly POSTPROCESS_DIR="$TAPPAS_WORKSPACE/apps/h8/gstreamer/libs/post_processes"
    readonly RESOURCES_DIR="$TAPPAS_WORKSPACE/apps/h8/gstreamer/general/instance_segmentation/resources"

    readonly DEFAULT_POSTPROCESS_SO="$POSTPROCESS_DIR/libyolov5seg_post.so"
    #DEFAULT_VIDEO_SOURCE="rtsp://admin:Aol2021!@192.168.1.64:554/Streaming/channels/101"
    readonly DEFAULT_VIDEO_SOURCE="$RESOURCES_DIR/instance_segmentation.mp4"
    readonly DEFAULT_HEF_PATH="$RESOURCES_DIR/yolov5n_seg.hef"
    readonly DEFAULT_NETWORK_NAME="yolov5seg"
    readonly json_config_path="$RESOURCES_DIR/configs/yolov5seg.json"
    PYTHON_MODULE_PATH="$TAPPAS_WORKSPACE/apps/h8/gstreamer/general/instance_segmentation/main.py"
    #KAFKA_PRODUCER="$TAPPAS_WORKSPACE/apps/h8/gstreamer/general/instance_segmentation/kafka_producer.py"
    #DB_PATH="$TAPPAS_WORKSPACE/apps/h8/gstreamer/general/instance_segmentation/detections.db"
    TRAIN_MODEL="$TAPPAS_WORKSPACE/apps/h8/gstreamer/general/instance_segmentation/train_knn_model.py"

    #python3 $TRAIN_MODEL

    postprocess_so=$DEFAULT_POSTPROCESS_SO
    network_name=$DEFAULT_NETWORK_NAME
    video_source=$DEFAULT_VIDEO_SOURCE
    hef_path=$DEFAULT_HEF_PATH

    print_gst_launch_only=false
    additional_parameters=""
    video_sink_element=$(echo "ximagesink")

    #video_sink_element=$([ "$XV_SUPPORTED" = "true" ] && echo "xvimagesink" || echo "ximagesink")

}

function print_usage() {
    echo "Sanity hailo pipeline usage:"
    echo ""
    echo "Options:"
    echo "  -h --help               Show this help"
    echo "  -i INPUT --input INPUT  Set the video source (default $video_source)"
    echo "  --show-fps              Print fps"
    echo "  --print-gst-launch      Print the ready gst-launch command without running it"
    exit 0
}

function print_help_if_needed() {
    while test $# -gt 0; do
        if [ "$1" = "--help" ] || [ "$1" == "-h" ]; then
            print_usage
        fi

        shift
    done
}

function parse_args() {
    while test $# -gt 0; do
        if [ "$1" = "--print-gst-launch" ]; then
            print_gst_launch_only=true
        elif [ "$1" = "--show-fps" ]; then
            echo "Printing fps"
            additional_parameters="-v | grep hailo_display"
        elif [ "$1" = "--input" ] || [ "$1" == "-i" ]; then
            video_source="$2"
            shift
        else
            echo "Received invalid argument: $1. See expected arguments below:"
            print_usage
            exit 1
        fi

        shift
    done
}

# function create_rtsp_source() {
#     rtspsrc_location="rtsp://admin:Aol2021\!@192.168.1.65:554/Streaming/channels/101"
#     rtph_depay="rtph264depay"
#     avdec="avdec_h264"

#     source_element="rtspsrc location=$rtspsrc_location name=source_0 ! \
#                 rtponvifparse ! \
#                 $rtph_depay ! \
#                 queue name=hailo_decode0 leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
#                 $avdec ! \
#                 queue name=hailo_scale0 leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
#                 videoscale ! videoconvert"
# }

init_variables $@
parse_args $@
create_rtsp_source

# If the video provided is from a camera
if [[ $video_source =~ "/dev/video" ]]; then
    source_element="v4l2src device=$video_source name=src_0 ! videoflip video-direction=horiz"
else
    source_element="filesrc location=$video_source name=src_0 ! decodebin"
fi

PIPELINE="gst-launch-1.0 \
    $source_element ! \
    videoscale ! video/x-raw, pixel-aspect-ratio=1/1 ! videoconvert ! \
    queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
    hailonet hef-path=$hef_path ! \
    queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
    hailofilter function-name=$network_name so-path=$postprocess_so config-path=$json_config_path qos=false ! \
    queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
    hailopython qos=false module=$PYTHON_MODULE_PATH function=run ! \
    queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
    hailooverlay qos=false ! \
    queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
    videoconvert ! \
    fpsdisplaysink video-sink=$video_sink_element name=hailo_display sync=false text-overlay=false ${additional_parameters}"

echo "Running $network_name"
#echo ${PIPELINE}

if [ "$print_gst_launch_only" = true ]; then
    echo ${PIPELINE}
    exit 0
else
    echo "Running pipeline"
    eval ${PIPELINE} 
    #python3 $KAFKA_PRODUCER &
    wait
fi

