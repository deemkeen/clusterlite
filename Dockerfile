FROM scratch

WORKDIR /data

# Copy the static binary
COPY ./clusterlite /data/clusterlite

EXPOSE 8082

# Run the binary
ENTRYPOINT [ "./clusterlite"]
