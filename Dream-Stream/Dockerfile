FROM mcr.microsoft.com/dotnet/core/aspnet:3.0.1-alpine3.10-arm64v8 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443
#RUN apk update && apk add --no-cache gcompat

FROM mcr.microsoft.com/dotnet/core/sdk:3.0-alpine AS build
WORKDIR /src
COPY ["Dream-Stream/Dream-Stream.csproj", "Dream-Stream/"]
RUN dotnet restore -r linux-arm64 "Dream-Stream/Dream-Stream.csproj"
COPY . .
WORKDIR "/src/Dream-Stream"
RUN dotnet build "Dream-Stream.csproj" -r linux-arm64 -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Dream-Stream.csproj" -r linux-arm64 -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Dream-Stream.dll"]