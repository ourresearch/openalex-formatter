for (( i=1; i<=$EXPORT_WORKERS_PER_DYNO; i++ ))
do
#   COMMAND="python queue_pub.py --run --name=run-$DYNO:${i} "
  COMMAND="python export_worker.py"
  echo $COMMAND
  $COMMAND&
done
trap "kill 0" INT TERM EXIT
wait