import pandas as pd
import re

from metaflow import FlowSpec, Parameter, step

class DiscussionFlow(FlowSpec):
    """
    Parse the discussions scraped from canvas
    """

    remove_index = Parameter('remove',
                      help="The index of the assignment description",
                      default=2)

    @step
    def start(self):
        """
        Load the discussions
        """

        # Load the discussions
        discussions = pd.read_csv('canvas.csv')

        # Drop unused columns
        discussions = discussions.drop(['web-scraper-order', 'web-scraper-start-url'], 1)

        # Remove the discussion description
        self.discussions = discussions.drop(discussions.index[self.remove_index]).reset_index(drop=True)

        self.next(self.clean)

    @step
    def clean(self):
        """
        Clean the discussion posts
        """

        # Remove all the text after the 'Edited by' text
        def clean_string(text):
            return re.sub(r'(?=Edited).*$', ' ', text.replace('\n', ' ')).strip()

        # Clean the text
        self.discussions['discussion'] = self.discussions['discussion_subentries'].apply(clean_string)

        # Drop the discussion_subentries column
        self.discussions = self.discussions.drop(['discussion_subentries'], 1)

        self.next(self.end)

    @step
    def end(self):
        """
        Store the dataframe
        """

        print(f'Parsed {len(self.discussions)} discussions')

        self.discussions.to_csv('canvas_parsed.csv', index=False)

if __name__ == '__main__':
    DiscussionFlow()