end_seconds=$(date -j -f "%Y-%m-%d" $1 +"%s")
declare -i days_back=0
declare -i delta=1

while [ $(date -v-${days_back}d +"%s" ) -gt $end_seconds ]; do
    prev_day=$(( $days_back + $delta ))
    end_date=$(date -v-${days_back}d +"%Y-%m-%d" )
    start_date=$(date -v-${prev_day}d +"%Y-%m-%d" )
    echo "Downloading "$start_date" - "$end_date" as: broker_${start_date}.txt"
    psql $DATA_BROKER_DATABASE_URL -o broker_${start_date}.txt -v ON_ERROR_STOP=1 -c "COPY (SELECT detached_award_procurement_id FROM detached_award_procurement where updated_at BETWEEN '${start_date}' AND '${end_date}') TO STDOUT"

    days_back=$prev_day;
done