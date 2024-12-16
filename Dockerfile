FROM scratch

# Create a volume for data persistence
#VOLUME ["/data"]

WORKDIR /data

# Copy the static binary
COPY ./clusterlite /data/clusterlite

EXPOSE 8082

# Run the binary
ENTRYPOINT [ "./clusterlite"]
