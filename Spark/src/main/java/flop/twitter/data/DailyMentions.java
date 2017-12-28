package flop.twitter.data;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "SecurityMediaMentions")
public class DailyMentions {
	private String symbol;
	private Integer goodMedia;
	private Integer badMedia;
	private Integer totalMedia;
	private String date;
	private Integer soloMedia;
	private Double sentimentScore; 
	
	public DailyMentions(String date, String symbol, int totalMedia){
		setSymbol(symbol);
		setDate(date);
		setTotalMedia(totalMedia);
	}
	
	@DynamoDBHashKey(attributeName = "symbol")
	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	@DynamoDBAttribute(attributeName = "goodMedia")
	public Integer getGoodMedia() {
		return goodMedia;
	}

	public void setGoodMedia(Integer goodMedia) {
		this.goodMedia = goodMedia;
	}

	@DynamoDBAttribute(attributeName = "badMedia")
	public Integer getBadMedia() {
		return badMedia;
	}

	public void setBadMedia(Integer badMedia) {
		this.badMedia = badMedia;
	}

	@DynamoDBAttribute(attributeName = "totalMedia")
	public Integer getTotalMedia() {
		return totalMedia;
	}

	public void setTotalMedia(Integer totalMedia) {
		this.totalMedia = totalMedia;
	}

	@DynamoDBRangeKey(attributeName = "date")
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@DynamoDBAttribute(attributeName = "soloMedia")
	public Integer getSoloMedia() {
		return soloMedia;
	}

	public void setSoloMedia(Integer soloMedia) {
		this.soloMedia = soloMedia;
	}
	
	@DynamoDBAttribute(attributeName = "sentimentScore")
	public Double getSentimentScore() {
		return sentimentScore;
	}

	public void setSentimentScore(Double sentimentScore) {
		this.sentimentScore = sentimentScore;
	}

}
